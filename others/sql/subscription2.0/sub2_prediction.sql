with


one_dave_new_members AS (
  SELECT
    *
  FROM
    ANALYTIC_DB.DBT_metrics.one_dave_new_members
),
sub_new AS (
  SELECT
    *
  FROM
    DAVE.subscription.subscription sub
),
sc_charge_new AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
attempts_new AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.subscription_charge_attempt
),
sc_status_new AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.subscription_charge_status
),
tier_new AS (
  SELECT
    *
  FROM
    DAVE.subscription.tier
),
payment_method AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.PAYMENT_METHOD_KIND
),
subscriber_new AS (
  SELECT
    sub.USER_ID,
    sub.ID AS SUBSCRIPTION_ID,
    tier.ID AS tier_id,
    started
  FROM
    sub_new sub
    JOIN tier_new tier ON sub.tier_id = tier.id
  WHERE
    tier.code = 'one_dollar'
    AND sub._DELETED = FALSE
),
sub_charge_new AS (
  SELECT
    DATE_TRUNC(
      'MONTH',
      CONVERT_TIMEZONE(
        'UTC',
        'America/Los_Angeles',
        sc_charge.term_started
      )
    ) AS billing_cycle,
    DATE(
      CONVERT_TIMEZONE(
        'UTC',
        'America/Los_Angeles',
        sc_charge.term_started
      )
    ) AS term_started,
    subscriber.user_id,
    subscriber.started,
    sc_charge.id AS subscription_charge_id,
    sc_status.code AS collect_status,
    payment.kind AS payment_final,
    sc_charge.unit_cost,
    sc_charge._deleted
  FROM
    subscriber_new subscriber
    JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
  WHERE
    term_started >= date('2022-06-01') --WHERE (sc_charge._DELETED = FALSE or sc_charge._DELETED IS NULL)
    qualify row_number() over (
      PARTITION by billing_cycle,
      user_id
      ORDER BY
        term_started
    ) = 1
),
last_attempt_new AS (
  SELECT
    subscription_charge_id,
    date(
      max(
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created)
      )
    ) AS last_attempt_date
  FROM
    attempts_new
  GROUP BY
    1
),
sub_combine AS (
  SELECT
    s.billing_cycle,
    s.subscription_charge_id,
    s.user_id,
    s.payment_final AS payment_final,
    s.term_started,
    s.unit_cost,
    s.collect_status,
    la.last_attempt_date,
    CASE
      WHEN s._deleted = TRUE THEN 'deleted'
      WHEN s.subscription_charge_id IS NULL THEN 'not_billed'
      WHEN la.last_attempt_date IS NULL
      AND collect_status IS NULL THEN 'not_attempted'
      WHEN s.collect_status <> 'collected' THEN s.collect_status
      WHEN s.collect_status = 'collected'
        AND s.unit_cost = 0 THEN 'free_succeeded'
      WHEN s.collect_status = 'collected'
        AND s.unit_cost > 0
        AND date_trunc('month', la.last_attempt_date) = s.billing_cycle THEN 'success_samemonth'
      WHEN s.collect_status = 'collected'
        AND s.unit_cost > 0
        AND date_trunc('month', la.last_attempt_date) > s.billing_cycle THEN 'success_nextmonth'
      ELSE 'others'
    END AS collect_status_update
  FROM
    sub_charge_new s
    LEFT JOIN last_attempt_new la ON la.subscription_charge_id = s.subscription_charge_id
    LEFT JOIN one_dave_new_members od ON s.user_id = od.user_id

)
,sub_predict as (SELECT
  *
FROM
  sub_combine where unit_cost>0
)
,oct as
(
    select
    user_id,
    collect_status,
    coalesce(last_attempt_date, current_date) as last_attempt,
    term_started
    from sub_predict
    where collect_status <> ' deleted' and (collect_status = 'pending_collection' or (collect_status in ('collected', 'failed_collection') and date(term_started) < date(last_attempt)))
    and billing_cycle = date_trunc('month', dateadd('month', -1, current_date()))
),
datelimit as
(
    select current_date() as current_date, dateadd('month', -1, current_date()) as last_date

),
recursive_attempt_date as
(
    select
        0 as startnumber,
        31 as endnumber
),
cte_attempt_date(ref_number) as
(
    select
        startnumber as ref_number
        from recursive_attempt_date
        union all
        select
        ref_number + 1 as ref_number
        from cte_attempt_date
        cross join
        recursive_attempt_date
        where ref_number < endnumber
),
numberlist as
(
  select ref_number from cte_attempt_date
),
octupdate as
(
    select *, datediff('day', term_started, last_attempt) as datediff from oct
)
,octgroup as (
    select datediff, count(distinct user_id) as total,
  count(
    case
    when collect_status = 'collected' then user_id
    else null
    end
    ) as success
  from octupdate
  where term_started <= dateadd('month', -1, current_date())
  group by 1
)
,number_oct as
(
    select
  a.ref_number,
  b.datediff,
  b.total,
  b.success

    from numberlist a
    left join octgroup b
    on a.ref_number <= datediff
),
rate as
(
    select ref_number, sum(success)/sum(total) as rate from number_oct group by 1
)
,oct_later as
(
    select * from sub_predict
    where term_started >= dateadd('month', -1, current_date())
  and billing_cycle = date_trunc('month', dateadd('month', -1, current_date()))
  and  collect_status <> ' deleted'
),
oct_later_rate as
(
    select count(
    case
      when collect_status = 'collected' then user_id
      else null
      end ) / count(distinct user_id) as later_rate
    from oct_later
),
ratecombine as
(
    select * from rate
    union
  select -1 as ref_number, later_rate as rate from oct_later_rate
)
,oct_pending as
(
    select * from sub_predict
    where term_started > dateadd('month', -1, current_date())
      and billing_cycle = date_trunc('month', dateadd('month', -1, current_date()))
      and  collect_status <> ' deleted'
      and collect_status = 'pending_collection'
)
,nov as (
    select
    user_id,
    collect_status,
    term_started,

    datediff('day', term_started, current_date()) as datediff,
  case
  when datediff >=0 then datediff
  else -1
  end as datediffupdate
    from sub_predict
    where billing_cycle = date_trunc('month', current_date())
  union all
  select
    user_id,
    'pending_predict' as collect_status,
    dateadd('month', 1, term_started) as term_started,
    29 as datediff,
   29 as datediffupdate
  from oct_pending
),
novgroup as
(
    select
    term_started,
    datediffupdate,
  count(
    case
    when collect_status = 'collected' then user_id
    else null
    end
  ) as success,
  count(
  case
    when collect_status like '%pending%' then user_id
    else null
    end
  ) as pending,
  count(
  case
    when collect_status = 'failed_collection' then user_id
    else null
    end
  ) as failed
  from nov
  group by 1, 2

)
,novgrouprate as
(
    select
  novgroup.term_started,
  novgroup.datediffupdate,
  novgroup.success,
  novgroup.pending,
  novgroup.failed,
  novgroup.pending * ratecombine.rate as prediction,
  novgroup.success + prediction as predictiontotal
    from novgroup
    left join ratecombine
  on novgroup.datediffupdate = ratecombine.ref_number
)
select * from novgrouprate