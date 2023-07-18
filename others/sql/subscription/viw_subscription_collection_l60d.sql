CREATE OR REPLACE VIEW DBT.DEV_HU_PUBLIC.view_subscription_collection_l60d AS
WITH attempts AS (
    -- attempts which reached payment processor
    SELECT
        DATE(created) AS attempt_date,
        subscription_billing_id AS bill_id,
        subscription_payment_id AS payment_id
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    WHERE attempt_date >= CURRENT_DATE - 60
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
)

SELECT
    a.attempt_date,
    DAYOFWEEK(a.attempt_date) AS attempt_day_of_week, // O: sunday, 1: monday, ..., 6: satruday
    CASE WHEN attempt_day_of_week = 0 OR attempt_day_of_week = 6 THEN 'weekend' ELSE 'weekday' END AS weekend_flag,
    COALESCE(p.status, 'FAILED') AS payment_status,
    p.external_processor,
    CASE external_processor
        WHEN 'TABAPAY' THEN 'Debit'
        WHEN 'SYNAPSEPAY' THEN 'ACH'
        ELSE external_processor
    END AS payment_method,
    COUNT(a.*) AS attempt_cnt,
    COUNT(a.bill_id) AS attempt_bill_cnt
FROM attempts a
LEFT JOIN payments p ON a.payment_id = p.payment_id
GROUP BY 1,2,3,4,5,6
;
