--create or replace table sandbox.dev_yyang.legacy_oct20 as
WITH subscription_billing AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_BILLING

),

user AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER

),

one_dave_new_members AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_new_members

),

payment_method AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.PAYMENT_METHOD

),

subscription_collection_attempt AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT

),

subscription_payment AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT

),

subscribers AS (
    -- monthly subscribers, starting from Sept 2020
    -- one user can only have one bill in a billing cycle
    SELECT
        b.billing_cycle,
        b.user_id,
        b.id AS bill_id,
        b.due_date AS bill_due_date,
        od.event_ds AS reg_date, -- date when users become one dave member
        u.subscription_fee,
        TO_VARCHAR(od.EVENT_DS, 'YYYY-MM') as reg_month,
        CASE WHEN reg_month < b.billing_cycle THEN 1 ELSE 0 END AS is_existing_subscriber
    FROM subscription_billing b
    INNER JOIN user u ON u.id = b.user_id
    LEFT JOIN one_dave_new_members od ON u.id = od.user_id -- subscribers must be One Dave members
    WHERE b.billing_cycle >= '2023-01'
        AND b.amount > 0
        AND b._fivetran_deleted = false
        AND u._fivetran_deleted = false
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

attempts AS (
    -- attempts which reached payment processor
    SELECT
        created,
        subscription_billing_id AS bill_id,
        subscription_payment_id AS payment_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS attempt_pt_ts,
        DATE(attempt_pt_ts) AS attempt_pt_dt
    FROM subscription_collection_attempt
    WHERE attempt_pt_dt >= '2023-01-01'
),

payments AS (
    -- payment succeeded
    SELECT
        id AS payment_id,
        created,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS payment_pt_ts,
        DATE(payment_pt_ts) AS payment_pt_dt,
        external_processor,
        CASE external_processor
            WHEN 'TABAPAY' THEN 'Debit'
            WHEN 'SYNAPSEPAY' THEN 'ACH'
            ELSE 'undetermined'
        END AS payment_method,
        user_id,
        amount
    FROM subscription_payment
    WHERE payment_pt_dt >= '2023-01-01'
        AND status in ('COMPLETED') -- check if pending should be included
        AND _FIVETRAN_DELETED = false
),

bill_agg AS (
    -- join all the CTEs above to get a summary view of bills
    SELECT
        s.billing_cycle,
        s.user_id,
        s.bill_id,
        s.bill_due_date,
        s.reg_date,
        s.reg_month,
        s.is_existing_subscriber,
        s.subscription_fee,
        IFF(d.user_id IS NOT NULL, 1, 0) AS has_debit_card,

        MAX(IFF(a.bill_id IS NOT NULL, 1, 0)) AS attempted_to_collect,
        MAX(IFF(p.payment_id IS NOT NULL, 1, 0)) AS is_collect_succeeded,

        -- update on 11/19/21: add first attempt and payment date
        MIN(a.attempt_pt_dt) AS first_attempt_pt_dt,
        MIN(p.payment_pt_dt) AS payment_pt_dt,

        -- update on 12/1/2021: add attempt counts
        COALESCE(COUNT(a.bill_id), 0) AS attempt_cnt,

        -- update on 12/10/2021: add payment method
        MAX(p.payment_method) AS payment_method
    FROM subscribers s
    LEFT JOIN debit_card_user d ON s.user_id = d.user_id
    LEFT JOIN attempts a ON s.bill_id = a.bill_id
    LEFT JOIN payments p ON a.payment_id = p.payment_id
    GROUP BY 1,2,3,4,5,6,7,8,9
)
select * from bill_agg


-----Paused user

 create or replace table sandbox.dev_yyang.paused_user as
 WITH
 bank_connection AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION

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

 paused_user as (
select USER_ID, PAUSED_AT, UNPAUSED_AT
from APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.MEMBERSHIP_PAUSE
   where PAUSED_AT <= CURRENT_DATE()
QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY PAUSED_AT DESC) = 1
 ),
 paused_update as (
 select * from paused_user where unpaused_at <= current_date()
 ),
 payment as (

   SELECT
        id AS payment_id,
        created,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS payment_pt_ts,
        DATE(payment_pt_ts) AS payment_pt_dt,
        external_processor,
        CASE external_processor
            WHEN 'TABAPAY' THEN 'Debit'
            WHEN 'SYNAPSEPAY' THEN 'ACH'
            ELSE 'undetermined'
        END AS payment_method,
        user_id,
        amount
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    WHERE payment_pt_dt >= dateadd('day',-30, current_date()) and payment_pt_dt <= current_date()
        AND status in ('COMPLETED') -- check if pending should be included
        AND _FIVETRAN_DELETED = false
   qualify row_number() over (partition by user_id order by payment_pt_ts desc) = 1
 )
 select
 a.*,
 case
 when b.user_id is null then 'not_paused'
 else 'paused'
 end as if_paused_user,
 b.paused_at,
 case
 when c.user_id is null then 'not_payment_within_30days'
 else 'payment_within_30days'
 end as if_payment_within_30_days,
c.payment_pt_dt as payment_time_fromtable,
 case
 when d.deleted <= current_date() or d.subscription_fee = 0 then 'deleted_or_fee_zero'
 else 'not_deleted'
 end as if_deleted_or_sub_cancel,
 e.user_bc_healthiness_score as bank_connection_healthy

 from sandbox.dev_yyang.users_inlegacy a
 left join paused_update b
 on a.user_id = b.user_id
 left join payment c
 on a.user_id = c.user_id
 left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER d
 on a.user_id = d.id
 left join user_bank_connection_healthiness e
 on a.user_id = e.user_id