---Attempt
WITH
one_dave_new_members AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_new_members

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
    from sub_new sub
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
        sc_status.code as collect_status
    FROM subscriber_new subscriber
    LEFT JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    WHERE sc_charge.unit_cost > 0 and (sc_charge._DELETED = FALSE or sc_charge._DELETED IS NULL )
),
last_attempt as (
    SELECT
        attempt_dt,
        subscription_charge_id,
        id as subscription_attempt_id
    FROM attempts_new
    Qualify row_number() OVER (PARTITION BY subscription_charge_id ORDER BY attempt_dt DESC) = 1
),
final as (
    select
        a.id as subscription_attempt_id,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED)) AS attempt_date,
        payment.kind as payment_attempt,
        a.subscription_charge_id,
        sc.collect_status as attempt_final_status,
        case
            when la.subscription_attempt_id is not null then 1
            else 0
        end as if_last_attempt,
        sca.billing_cycle,
        sca.term_started,
        sca.user_id
    from attempts_new a join last_attempt la
    on a.id = la.subscription_attempt_id
    left join payment_method payment
    on a.payment_method_kind_id = payment.id
    left join sub_charge_new sc on la.subscription_charge_id = sc.subscription_charge_id
    left join sub_charge_new sca on a.subscription_charge_id = sca.subscription_charge_id
)
select * from final









----Hu's script

with daily_attempts AS (
    SELECT
        DISTINCT
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created)::date AS attempt_dt,
            subscription_charge_id,
            id as subscription_attempt_id
    FROM DAVE.SUBSCRIPTION.subscription_charge_attempt
    WHERE DELETED IS NULL
        AND _DELETED = false
),

daily_attempts_rnk AS (
    SELECT
        attempt_dt,
        subscription_charge_id,
        row_number() OVER (PARTITION BY subscription_charge_id ORDER BY attempt_dt DESC) AS rnk
    FROM daily_attempts
),
payment_method AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.PAYMENT_METHOD_KIND
),
final AS (
    SELECT
      attempts.attempt_dt,
      CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt,
      term_started_pt::date AS bill_start_dt, -- bill start date in PT
      scs.code AS collection_status, -- collected, pending_collection, failed_collection,
      IFF(bill_start_dt = attempts.attempt_dt, 1, 0) AS is_attempted_at_bill_day,
      IFF(attempts.rnk = 1 and collection_status = 'collected', 'collected', 'failed') AS attempt_status,
      payment.kind as payment_attempt
    FROM daily_attempts_rnk attempts
    LEFT JOIN DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE sc ON sc.id = attempts.subscription_charge_id AND sc._deleted = false
    LEFT JOIN DAVE.subscription.subscription sub ON sc.subscription_id = sub.id
    INNER JOIN DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar' -- 'one_dollar' only
    LEFT JOIN DAVE.SUBSCRIPTION.subscription_charge_status scs ON sc.subscription_charge_status_id = scs.id
    LEFT JOIN payment_method payment ON attempts.payment_method_kind_id = payment.id
    WHERE bill_start_dt <= current_date() -- bill is due
        AND sc.unit_cost > 0 -- not free
)
SELECT
  attempt_dt,
  bill_start_dt,
  is_attempted_at_bill_day,
  attempt_status,
  COUNT(*) AS cnt
FROM final
GROUP BY 1,2,3,4