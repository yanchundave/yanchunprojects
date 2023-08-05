---Sub2.0 monthly subscription
--b.billing_cycle,
--    b.user_id,
--    b.bill_id,
--    b.bill_due_date,
--    b.reg_date,
---    b.reg_month,
 --   b.is_existing_subscriber,
 --   b.bc_healthiness_score,
 ----   b.has_debit_card,
--    b.attempted_to_collect,
 --   b.is_collect_succeeded,
 --   b.first_attempt_pt_dt,
 --   b.payment_pt_dt, (not ready)
 --   b.attempt_cnt,  (not ready)
 --   b.payment_method,  (not ready)

WITH subscription AS
(
    SELECT * FROM DAVE.subscription.subscription
),

tier AS
(
    SELECT * FROM DAVE.subscription.tier
),

sc_charge AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),

attempt AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.subscription_charge_attempt
),

sc_status AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.subscription_charge_status
),

bank_connection AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION

),

payment_method AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.PAYMENT_METHOD

),

subscriber AS (
    SELECT
        subscription.USER_ID,
        subscription.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id,
        subscription.STARTED
    FROM subscription
    JOIN tier
    ON subscription.tier_id = tier.id
    WHERE tier.code = 'one_dollar'
        AND  subscription._DELETED = FALSE

),
sc_monthly_charge AS (
    SELECT
        DATE_TRUNC('MONTH',  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) AS billing_cycle,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) as billing_date,
        subscriber.user_id,
        sc_charge.id AS subscription_charge_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_ended) as bill_due_date,
        subscriber.started AS reg_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', subscriber.started)) AS reg_month,
        case when reg_month < billing_cycle THEN 1 ELSE 0 END AS is_existing_subscriber,
        sc_status.code,
        sc_charge.unit_cost
    FROM subscriber
    JOIN sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
),
bank_connection_healthiness AS (
    -- healthiness of individual bank connections (at bank connection level)
    SELECT
        user_id,
        id AS connection_id,
        CASE
            WHEN banking_data_source_error_code in ('ITEM_NOT_FOUND',
                                                    'ACCESS_NOT_GRANTED',
                                                    'INSTITUTION_NOT_FOUND',
                                                    'INSTITUTION_NO_LONGER_SUPPORTED',
                                                    'ITEM_NOT_SUPPORTED',
                                                    'NO_ACCOUNTS',
                                                    'ITEM_LOCKED')
                THEN 3 -- 'Disconnected'
            WHEN has_valid_credentials = FALSE OR banking_data_source_error_code in ('INSUFFICIENT CREDENTIALS',
                                                    'INVALID_CREDENTIALS',
                                                    'INVALID_MFA',
                                                    'INVALID_SEND_METHOD',
                                                    'ITEM_LOGIN_REQUIRED',
                                                    'USER_SETUP_REQUIRED',
                                                    'MFA_NOT_SUPPORTED',
                                                    'INSUFFICIENT_CREDENTIALS')
                THEN 2 -- 'Unhealthy'
            ELSE 1 -- 'Healthy'
            END AS bc_healthiness_score
    FROM bank_connection
    WHERE deleted IS NULL
        AND _fivetran_deleted = false
        AND banking_data_source = 'PLAID'
),

user_bank_connection_healthiness AS (
    -- healthiness of bank connections at user level
    SELECT
        user_id,
        MIN(bc_healthiness_score) AS user_bc_healthiness_score
    FROM bank_connection_healthiness
    GROUP BY 1
),

debit_card_user AS (
    -- users who have valid debit cards
    SELECT
        DISTINCT user_id
    FROM payment_method
    WHERE INVALID IS NULL
          AND DELETED IS NULL
          AND EXPIRATION > CURRENT_DATE()
          AND _FIVETRAN_DELETED = false
),

sub_attempts AS (
    SELECT
        SUBSCRIPTION_CHARGE_ID,
        COUNT(DISTINCT ID) AS attempt_cnt,
        MIN(CREATED) AS first_attempt_pt_dt,
        MAX(CREATED) AS last_attempt_pt_dt
    FROM attempt
    GROUP BY 1
),

sub_agg AS (

    SELECT
        s.billing_cycle,
        s.billing_date,
        s.user_id,
        s.subscription_charge_id,
        s.bill_due_date,
        s.reg_date,
        s.reg_month,
        s.is_existing_subscriber,
        s.unit_cost,
        u.user_bc_healthiness_score AS bc_healthiness_score,
        IFF(d.user_id IS NOT NULL, 1, 0) AS has_debit_card,
        IFF(a.subscription_charge_id IS NOT NULL, 1, 0) AS attempted_to_collect,
        s.code  AS collection_status,
        coalesce(a.attempt_cnt, 0) as attempt_cnt,
        a.first_attempt_pt_dt,
        a.last_attempt_pt_dt as last_attempt_date,
        IFF(a.last_attempt_pt_dt > bill_due_date, 1, 0) AS pay_before_due,
        'SUB2.0' as product
    FROM sc_monthly_charge s
    LEFT JOIN user_bank_connection_healthiness u
    ON s.user_id = u.user_id
    LEFT JOIN debit_card_user d
    on s.user_id = d.user_id
    LEFT JOIN sub_attempts a
    on s.subscription_charge_id  = a.subscription_charge_id
)

select * from sub_agg