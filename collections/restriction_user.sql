--R01 restricted users and transaction

WITH RESTRICTED AS
(
  SELECT
    ACHRETURNCODE,
    DAVEUSERID,
    REPLACE(F.value, '\"') AS TRANSACTIONID --ACH TRANSACTION
  FROM DAVE.LOOMIS.RESTRICTION A,
  Table(Flatten(A.TRANSACTIONIDS)) F
  WHERE ACHRETURNCODE = 'R01'
  AND DATE(CREATED) >= DATE('2022-01-01')
),
TRANS_REFERENCE AS (
  SELECT
    A.*,
    B.REFERENCEID
  FROM RESTRICTED A
  JOIN LOOMIS_DB.LOOMIS.TRANSACTION B
  ON A.TRANSACTIONID = B.TRANSACTIONID
),
O2 AS (
  SELECT
    A.*,
    B.OVERDRAFT_ID,
    B.SETTLEMENT_STATUS_ID,
    C.NAME
  FROM TRANS_REFERENCE A
  JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT B
  ON A.REFERENCEID = B.ID
  JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT_STATUS C
  ON B.SETTLEMENT_STATUS_ID = C.ID
),
O2_TASK AS (
  SELECT
    O2.*,
    TASK.TASK_ID
    FROM O2
    JOIN APPLICATION_DB.TIVAN.TASK_EXTERNAL_REFERENCE TASK
    ON O2.OVERDRAFT_ID = TASK.EXTERNAL_ID
),
O2_TASKATTEMPT AS (
  SELECT
    O2_TASK.*,
    ATTEMPT.TASK_ATTEMPT_ID,
    --Success (1), Failure (2), Pending (3), Error (4), Retry (5), and Locked (6)
    ATTEMPT.RESULT
  FROM O2_TASK
  JOIN APPLICATION_DB.TIVAN.TASK_ATTEMPT_RESULT ATTEMPT
  ON O2_TASK.TASK_ID = ATTEMPT.TASK_ID
),
EC_manual_settlement_dr AS (
  SELECT
        b.total_due
       ,b.outstanding_status
       ,b.disbursed_amount
       ,b.created_date
       ,b.due_date
       ,b.disbursed_amount_category
       ,case when date(a.created)<date(b.due_date) then 'a: before due'
             when date(a.created)>=date(b.due_date) then 'b: on or after due'
             else 'c: others' end as pay_vs_due_flg
       ,datediff(day, date(b.created_date),date(a.created)) as num_days_diff_pay_vs_disbursement
       ,case when a.amount>=total_due then 'a: full pay_off'
             else 'b: partial_pay'
        end as pay_full_vs_partial
       ,CASE WHEN DATEDIFF(DAY, U.PV_TS, current_date) <= 30 THEN 'a: user_created_last_30d'
             WHEN DATEDIFF(DAY, U.PV_TS, current_date) > 30 THEN 'b: user_crated_more_than_30d_ago'
        END AS USER_TYPE
        ,a.SETTLEMENT_METHOD_LOOMIS_ID,
        ,a.SETTLEMENT_STATUS_ID,
        ,a.TRIGGER_TYPE,
        ,a.chargeback_status_id
  FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT a
  JOIN ANALYTIC_DB.DBT_marts.fct_ExtraCash_Collection b
    ON a.overdraft_id=b.overdraft_id
  LEFT JOIN ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION U
    ON U.USER_ID = b.USER_ID
  WHERE date(b.created_date)>='2022-02-01' and date(a.created)>='2022-02-01'  and date(created_date)<current_date
),
COMBINED AS (
  SELECT
    A.overdraft_id,
    A.total_due,
    A.outstanding_status,
    A.disbursed_amount,
    A.created_date,
    A.due_date,
    A.disbursed_amount_category,
    A.pay_vs_due_flg,
    A.num_days_diff_pay_vs_disbursement,
    A.pay_full_vs_partial,
    A.USER_TYPE,
    A.SETTLEMENT_METHOD_LOOMIS_ID,
    A.SETTLEMENT_STATUS_ID,
    A.TRIGGER_TYPE,
    A.chargeback_status_id,
    B.DAVEUSERID,
    B.TRANSACTIONID,
    B.NAME AS SETTLEMENT_STATUS,
    B.TASK_ID,
    B.TASK_ATTEMPT_ID,
    B.RESULT AS ATTEMPT_RESULT
  FROM EC_manual_settlement_dr A
  JOIN O2_TASKATTEMPT B
  ON A.OVERDRAFT_ID = B.OVERDRAFT_ID
)
SELECT * FROM COMBINED LIMIT 10;