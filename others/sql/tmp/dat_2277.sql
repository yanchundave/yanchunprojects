WITH attempts AS (
    -- attempts which reached payment processor
    SELECT
        DATE(created) AS attempt_date,
        subscription_billing_id AS bill_id,
        subscription_payment_id AS payment_id
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    WHERE attempt_date >= date('2023-06-26') and attempt_date <= date('2023-06-30')
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
where payment_method = 'ACH'
)
select payment_status, count(*) from tmp group by 1