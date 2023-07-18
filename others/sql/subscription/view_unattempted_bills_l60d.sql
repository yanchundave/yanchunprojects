CREATE OR REPLACE VIEW DBT.DEV_HU_PUBLIC.view_unattempted_bills_l60d AS
WITH
// bills are not attempted to collect (not reach to payment processor)
unattempted_bills AS (
  SELECT
      billing_cycle,
      user_id,
      bill_id,
      bill_due_date,
      is_existing_subscriber,
      bc_healthiness_score,
      has_debit_card,
      attempted_to_collect,
      is_collect_succeeded,
      payment_pt_dt
  FROM DBT.DEV_HU_PUBLIC.view_monthly_subscription_users
  WHERE attempted_to_collect = 0
    AND bill_due_date >= current_date - interval '60 days'
    AND bill_due_date < current_date
),

// balance log at due date
balance AS (
    SELECT
        user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', timestamp::timestamp_ntz)) AS balance_pt_dt,
        MAX(COALESCE(AVAILABLE, "CURRENT")) AS max_balance
    FROM SECONDARY_APP_DB.PUBLIC.BALANCE_LOG
    WHERE balance_pt_dt >= current_date - interval '60 days'
    GROUP BY 1,2
),

// audit log
audit_log AS (
  SELECT
      DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz)) AS audit_pt_dt,
      event_uuid AS bill_id,
      user_id,
      message AS error_message,
      type,
      extra:err:data:balance AS balance,
      ARRAY_TO_STRING(parse_json(extra:err:data:failures), ', ') AS failures,
      extra:err:errorName AS error_name,
      ROW_NUMBER() OVER (PARTITION BY bill_id ORDER BY created) AS row_number,
      COUNT(bill_id) OVER (PARTITION BY bill_id) AS total_audit_log_count
  FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_AUDIT_LOG_DAVE.AUDIT_LOG
  WHERE TYPE in ('SUBSCRIPTION_COLLECTION_JOB','PAST_DUE_SUBSCRIPTION_COLLECTION')
      AND _FIVETRAN_DELETED = 'FALSE'
      AND audit_pt_dt >= current_date - interval '60 days'
)

SELECT
    b.*,

//    balance data
    CASE
        WHEN ba.max_balance is NULL                          THEN 'NULL'
        WHEN ba.max_balance < 0                              THEN 'NEGATIVE'
        WHEN ba.max_balance >=  0  AND ba.max_balance < 1       THEN '0-1'
        WHEN ba.max_balance >=  1  AND ba.max_balance < 5       THEN '1-5'
        WHEN ba.max_balance >= 5   AND ba.max_balance < 10      THEN '5-10'
        WHEN ba.max_balance >= 10  AND ba.max_balance < 200     THEN '10-200'
        WHEN ba.max_balance >= 200                           THEN '200+'
    END AS balance_at_due_date_bucket,

//    audit log
    al.error_message AS first_audit_log_error_message,
    al.error_name AS first_audit_log_error_name,
    al.balance AS first_audit_log_balance,
    al.type AS first_audit_log_type,
    al.failures AS first_audit_log_failures,


    CASE
        WHEN al.balance is NULL                          THEN 'NULL'
        WHEN al.balance < 0                              THEN 'NEGATIVE'
        WHEN al.balance >=  0  AND al.balance < 1       THEN '0-1'
        WHEN al.balance >=  1  AND al.balance < 5       THEN '1-5'
        WHEN al.balance >= 5   AND al.balance < 10      THEN '5-10'
        WHEN al.balance >= 10                       THEN '10+'
    END AS first_audit_log_balance_bucket,
    total_audit_log_count
FROM unattempted_bills b
LEFT JOIN audit_log al ON al.bill_id = b.bill_id AND al.user_id = b.user_id AND al.audit_pt_dt = b.bill_due_date AND al.row_number = 1 // first audit log
LEFT JOIN balance ba ON b.user_id = ba.user_id AND b.bill_due_date = ba.balance_pt_dt