--Dashboard
-- https://app.mode.com/editor/dave_saves/reports/77a36ce22160/presentation
WITH subscription_billing AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_BILLING

),

user AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER

),

one_dave_new_members AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_new_members

),

bank_connection AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION

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

audit_log AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_AUDIT_LOG_DAVE.AUDIT_LOG

),

audit_log_2 AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_AUDIT_LOG_DAVE.AUDIT_LOG_2

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
        TO_VARCHAR(od.EVENT_DS, 'YYYY-MM') as reg_month,
        CASE WHEN reg_month < b.billing_cycle THEN 1 ELSE 0 END AS is_existing_subscriber
    FROM subscription_billing b
    INNER JOIN user u ON u.id = b.user_id
    LEFT JOIN one_dave_new_members od ON u.id = od.user_id -- subscribers must be One Dave members
    WHERE b.billing_cycle >= '2020-01'
        AND b.amount > 0
        AND b._fivetran_deleted = false
        AND u._fivetran_deleted = false
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

attempts AS (
    -- attempts which reached payment processor
    SELECT
        created,
        subscription_billing_id AS bill_id,
        subscription_payment_id AS payment_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS attempt_pt_ts,
        DATE(attempt_pt_ts) AS attempt_pt_dt
    FROM subscription_collection_attempt
    WHERE attempt_pt_dt >= '2020-01-01'
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
    WHERE payment_pt_dt >= '2020-01-01'
        AND status in ('COMPLETED','PENDING') -- check if pending should be included
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
        u.user_bc_healthiness_score AS bc_healthiness_score,
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
    LEFT JOIN user_bank_connection_healthiness u ON s.user_id = u.user_id
    LEFT JOIN debit_card_user d ON s.user_id = d.user_id
    LEFT JOIN attempts a ON s.bill_id = a.bill_id
    LEFT JOIN payments p ON a.payment_id = p.payment_id
    GROUP BY 1,2,3,4,5,6,7,8,9
),

audit_log_union AS (
    SELECT
        ID,
        USER_ID,
        TYPE,
        EVENT_UUID,
        MESSAGE,
        EXTRA,
        CREATED,
        EVENT_TYPE,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED,
        SUCCESSFUL
    FROM audit_log
    WHERE TYPE in ('SUBSCRIPTION_COLLECTION_JOB','PAST_DUE_SUBSCRIPTION_COLLECTION')
      AND _FIVETRAN_DELETED = false
      AND CREATED::date >=  '2020-01-01'

    UNION

    SELECT
        ID,
        USER_ID,
        TYPE,
        EVENT_UUID,
        MESSAGE,
        EXTRA,
        CREATED,
        EVENT_TYPE,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED,
        SUCCESSFUL
    FROM audit_log_2
    WHERE TYPE in ('SUBSCRIPTION_COLLECTION_JOB','PAST_DUE_SUBSCRIPTION_COLLECTION')
      AND _FIVETRAN_DELETED = false
      AND CREATED::date >=  '2020-01-01'
),

audit_log_final AS (
  -- audit log to get unattempt reasons
  SELECT
      DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz)) AS audit_pt_dt,
      event_uuid AS bill_id,
      user_id,
      message AS error_message,
      extra:err:errorName AS error_name,
      type,
      ARRAY_TO_STRING(parse_json(extra:err:data:failures), ', ') AS failures,

      -- extra:accountBalance is usually avaialbe when payment was attempted,
      -- extra:err:data:balance is usually available when payment was NOT attempted due to low balance
      COALESCE(extra:accountBalance, extra:err:data:balance) AS balance,
      CASE
          WHEN balance is NULL                          THEN '0. NULL'
          WHEN balance < 0                              THEN '1. <0'
          WHEN balance < 1       THEN '2. 0-1'
          WHEN balance < 5       THEN '3. 1-5'
          WHEN balance < 10      THEN '4. 5-10'
          WHEN balance < 50      THEN '5. 10-50'
          WHEN balance < 200     THEN '6. 50-200'
          WHEN balance < 500     THEN '7. 200-500'
          ELSE '8. 500+'
        END AS balance_bucket,

      ROW_NUMBER() OVER (PARTITION BY bill_id ORDER BY created) AS row_number,
      COUNT(bill_id) OVER (PARTITION BY bill_id) AS total_audit_log_count
    FROM audit_log_union
)

-- join audit log to get first collection trigger info

SELECT
    b.billing_cycle,
    b.user_id,
    b.bill_id,
    b.bill_due_date,
    b.reg_date,
    b.reg_month,
    b.is_existing_subscriber,
    b.bc_healthiness_score,
    b.has_debit_card,
    b.attempted_to_collect,
    b.is_collect_succeeded,
    b.first_attempt_pt_dt,
    b.payment_pt_dt,
    b.attempt_cnt,
    b.payment_method,

    IFF(b.attempted_to_collect = 0, al.error_message, NULL) AS first_trigger_error_message,
    IFF(b.attempted_to_collect = 0, al.error_name, NULL) AS first_trigger_error_name,
    IFF(b.attempted_to_collect = 0, al.failures, NULL) AS first_trigger_failures,
    IFF(b.attempted_to_collect = 0, al.total_audit_log_count, NULL) AS total_trigger_count,

    al.balance AS balance,
    COALESCE(al.balance_bucket, '0. NULL') AS balance_bucket,
    f.user_label AS advance_user_segment
FROM bill_agg b
LEFT JOIN audit_log_final al ON al.bill_id = b.bill_id AND al.user_id = b.user_id AND al.row_number = 1
LEFT JOIN dbt.adv_churn_marts.fct_adv_segment f ON b.bill_due_date = f.date_of_interest AND b.user_id = f.user_id