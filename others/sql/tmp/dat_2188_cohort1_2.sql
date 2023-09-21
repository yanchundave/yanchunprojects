--TOTAL PERFORMANCE
--Total subscribers (#)
--Total attempts (% of total subscribers)
----Total success (% of total attempts)
--Total fail (% of total attempts)
--ACH PERFORMANCE

--Balance checks (% of total subscribers)
--ACH attempts (% of balance checks)
--ACH success (% of ACH attempts)
--ACH fail (% of ACH attempts)
--ACH returns so far (# and % of ACH success)
--DEBIT PERFORMANCE
--Debit attempts (% of balance checks)
----Debit success (% of debit attempts)
--Debit fail (% of debit attempts)

-- work on legacy data  June 26 - July 10
--Total subscribers with bill dates in June

WITH attempts AS (
    -- attempts which reached payment processor
    SELECT
        DATE(created) AS attempt_date,
        subscription_billing_id AS bill_id,
        subscription_payment_id AS payment_id
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    WHERE attempt_date >= date('2023-06-26') and attempt_date <= date('2023-07-10')
),

payments AS (
    -- payment succeeded
    SELECT
        id AS payment_id,
        created,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz)) AS payment_pt_dt,
        external_processor,
        user_id,
        amount,
        status
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    WHERE payment_pt_dt >= CURRENT_DATE - 60
        AND _FIVETRAN_DELETED = 'FALSE'
),
tmp as (
SELECT
    a.attempt_date,
    a.bill_id,
    a.payment_id,
    COALESCE(p.status, 'FAILED') AS payment_status,
    p.external_processor,
    CASE external_processor
        WHEN 'TABAPAY' THEN 'Debit'
        WHEN 'SYNAPSEPAY' THEN 'ACH'
        ELSE external_processor
    END AS payment_method
FROM attempts a
LEFT JOIN payments p ON a.payment_id = p.payment_id

)
select payment_status, payment_method, count(*), count(distinct bill_id) from tmp group by 1, 2

----------------------------
--Total subscriber

with tmp as (
  select user_id, bill_id
    from analytic_db.dbt_marts.fct_monthly_subscription_users
    where (billing_cycle = '2023-06' or billing_cycle = '2023-07')
)
select
    count(distinct tmp.user_id)
from tmp
join APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT b
on tmp.bill_id = b.subscription_billing_id

------------subscription 2.0 cohort 1 + 2
WITH attempts AS (
    SELECT
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created) AS created_pt,
        subscription_charge_id,
        COUNT(*) OVER (PARTITION BY subscription_charge_id) AS attempt_cnt
    FROM DAVE.SUBSCRIPTION.subscription_charge_attempt
    WHERE DELETED IS NULL
        AND _DELETED = false
    QUALIFY row_number() OVER (PARTITION BY subscription_charge_id ORDER BY created DESC) = 1 -- only latest record will be included  without it, other subscription_charge_id is kept
),
users as (
    select distinct user_id
  from APPLICATION_DB.TRANSACTIONS_DAVE.USER_SUBSCRIPTIONS_2_0
  where added >= date('2023-06-20')
),
final as (

SELECT
    sc.subscription_id,
    sub.user_id,
    sc.id,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt,
    term_started_pt::date AS bill_start_dt, -- bill start date in PT
    scs.code AS collection_status, -- collected, pending_collection, failed_collection
    coalesce(attempts.attempt_cnt, 0) AS attempt_cnt,
    attempts.created_pt AS last_attempt_ts,
    attempts.created_pt::date AS last_attempt_dt,
    t.externalprocessor AS payment_processor,
    IFF(bill_start_dt = last_attempt_dt, 1, 0) AS is_attempted_at_bill_day,
    IFF(collection_status = 'collected', last_attempt_dt, NULL) AS collected_dt,
    -- IFF(bill_start_dt = collected_dt, 1, 0) AS is_collected_at_bill_day,
    CASE WHEN collected_dt IS NOT NULL AND bill_start_dt = collected_dt THEN 'collected at bill day'
        WHEN collected_dt IS NOT NULL THEN 'collected later'
        ELSE 'not collect'
        END AS collected_at_bill_day_flag,
    sc.unit_cost,
    t.transactionstatus AS loomis_txn_status
FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE sc
LEFT JOIN DAVE.subscription.subscription sub ON sc.subscription_id = sub.id
  --inner join users on sub.user_id = users.user_id
INNER JOIN DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar' -- 'one_dollar' only
LEFT JOIN DAVE.SUBSCRIPTION.subscription_charge_status scs ON sc.subscription_charge_status_id = scs.id
LEFT JOIN attempts ON sc.id = attempts.subscription_charge_id
LEFT JOIN dave.loomis.transaction t ON sc.reference_id = t.REFERENCEID
WHERE bill_start_dt <= current_date() -- bill is due
    AND sc.unit_cost > 0 -- not free
    AND sc._deleted = false
    and sub.started >= Date('2023-06-26') and sub.started <= Date('2023-07-10')
  )
select collection_status, sum(attempt_cnt), count(id) from final group by 1

---After June 20, 60413 users
select distinct user_id
  from APPLICATION_DB.TRANSACTIONS_DAVE.USER_SUBSCRIPTIONS_2_0
  where added >= date('2023-06-20')

--------------

