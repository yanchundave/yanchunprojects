with recursive_attempt_date as
(
    select
        Date_trunc('month', CURRENT_DATE() - 365) as start_date,
        current_date() as end_date
),
cte_attempt_date(ref_date) as
(
    select
        start_date as ref_date
        from recursive_attempt_date
        union all
        select
        ref_date + 1 as ref_date
        from cte_attempt_date
        cross join
        recursive_attempt_date
        where ref_date < end_date
),
start_month as
(
  select distinct date_trunc('month', ref_date) as start_date from cte_attempt_date
),
date_combine as (
    select
        a.start_date,
        b.ref_date as end_date
    from start_month a
    join cte_attempt_date b
    on a.start_date = date_trunc('month', b.ref_date)

),

sub_new as (
    select * from DAVE.subscription.subscription sub
),
sc_charge_new as (
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
attempts_new as (
    select * from DAVE.SUBSCRIPTION.subscription_charge_attempt
),
sc_status_new as (
    SELECT * FROM DAVE.SUBSCRIPTION.subscription_charge_status
),
tier_new AS
(
    SELECT * FROM DAVE.subscription.tier
),
payment_method AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.PAYMENT_METHOD_KIND
),
subscriber_new AS (
    SELECT
        sub.USER_ID,
        sub.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id
    FROM sub_new sub
    JOIN tier_new tier
    ON sub.tier_id = tier.id
    WHERE tier.code = 'one_dollar'
        AND  sub._DELETED = FALSE
),
sub_charge_new AS (
    SELECT
        DATE_TRUNC('MONTH',  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) AS billing_cycle,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) as term_started,
        subscriber.user_id,
        sc_charge.id AS subscription_charge_id,
        sc_status.code as collect_status,
        payment.kind,
        sc_charge.unit_cost
    FROM subscriber_new subscriber
    JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
),
sub_attempts_new AS (
    SELECT
        a.id as subscription_attempt_id,
        s.subscription_charge_id,
        a.created,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED)) AS attempt_date,
        DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED))) as monthnum,
        s.billing_cycle,
        s.user_id,
        s.kind as payment_method
    FROM attempts_new a
    JOIN sub_charge_new s on a.subscription_charge_id = s.subscription_charge_id
    WHERE a.created >= s.term_started
    order by monthnum, attempt_date, created
),
attempt_group_new as
(
    select
        billing_cycle,
        user_id,
        subscription_charge_id,
        attempt_date,
        monthnum,
        payment_method,
        count(distinct subscription_attempt_id) as attempt_cnt
    from sub_attempts_new
    group by 1, 2, 3, 4, 5, 6
),
last_attempt_new as (
    select
        subscription_charge_id,
        date(max(created)) as last_attempt_date
    from attempts_new
    group by 1
),
success_sub_new as (
    select
        sc.billing_cycle,
        sc.term_started,
        sc.user_id,
        sc.subscription_charge_id,
        sc.collect_status,
        sc.unit_cost / 100 as unit_cost,
        la.last_attempt_date
    from sub_charge_new sc
    join last_attempt_new la
    on sc.subscription_charge_id = la.subscription_charge_id
),
final_new as (
    select
        a.billing_cycle,
        a.user_id,
        a.subscription_charge_id,
        a.attempt_date,
        a.monthnum,
        a.attempt_cnt,
        s.unit_cost,
        iff(s.collect_status in ('collected', 'pending_collection'), 1, 0) as if_succeed,
        s.collect_status,
        a.payment_method
    from attempt_group_new a
    left join success_sub_new s
    on a.subscription_charge_id = s.subscription_charge_id
    and a.attempt_date = s.last_attempt_date
)
select * from final_new