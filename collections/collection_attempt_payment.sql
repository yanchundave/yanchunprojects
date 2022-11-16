--collections

DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT;
CREATE table DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT AS
WITH restricted AS
(
  SELECT
    DAVEUSERID,
    COUNT(DISTINCT REPLACE(F.value, '\"')) AS TRANSACTIONID --ACH TRANSACTION
  FROM DAVE.LOOMIS.RESTRICTION A,
  Table(Flatten(A.TRANSACTIONIDS)) F
  WHERE ACHRETURNCODE = 'R01'
  AND RESOLVED IS NULL
  GROUP BY DAVEUSERID
)
,advance_payment_webhook_base AS (
    SELECT
      advance_id,
      id as payment_id,
      amount as payment_amount,
      date(created) as payment_date,
      status,
      WEBHOOK_DATA,
      user_id
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.PAYMENT
    WHERE external_processor = 'SYNAPSEPAY'
      AND STATUS = 'RETURNED'
      AND EXTERNAL_ID IS NOT NULL
      AND WEBHOOK_DATA <> '[]'
)
,advance_payment_status_base AS (
    SELECT
      a.advance_id,
      a.payment_id,
      a.payment_amount,
      a.payment_date,
      a.status,
      a.user_id,
      b.VALUE :recent_status: date['$date'] as date_ts,
      b.VALUE :recent_status :code AS status_code
    FROM advance_payment_webhook_base a,
        TABLE(flatten(a.WEBHOOK_DATA)) b
    WHERE NOT IS_NULL_VALUE(status_code) -- variant null can't be handled by "is not"
  -- if status code is a variant null, that means the advance payment can't be R01 or R20 error
  -- even if the payment failed or advance loss
  qualify row_number() over (partition by a.payment_id order by date_ts desc) = 1
)
,restrict_payment as
(
   SELECT
        a.DAVEUSERID AS user_id,
        --a.ACHRETURNCODE,
        a.TRANSACTIONID,
        b.advance_id,
        b.payment_id,
        b.payment_amount,
        b.payment_date,
        b.status,
        b.date_ts,
        b.status_code
    FROM restricted a
    JOIN advance_payment_status_base b
      ON a.DAVEUSERID = b.user_id
)
, payment_attempt as
(
    SELECT
      a.user_id,
      --a.ACHRETURNCODE,
      a.TRANSACTIONID,
      a.advance_id,
      a.payment_id,
      a.payment_amount,
      a.payment_date,
      a.status,
      a.date_ts,
      a.status_code,
      --b.TRIGGER,
      b.id as attempt_id,
      b.created as attempt_time,
      b.updated as updated_time
    FROM restrict_payment a
    LEFT JOIN  APPLICATION_DB.TRANSACTIONS_DAVE.ADVANCE_COLLECTION_ATTEMPT b
      ON a.payment_id = b.payment_id
      AND a.advance_id = b.advance_id
)
SELECT
  a.*,
  b.outstanding
FROM payment_attempt a
LEFT JOIN ANALYTIC_DB.DBT_marts.fct_advances_Collection b
  ON a.advance_id = b.advance_id

-----Legacy advance users payments
DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_UPDATE;
CREATE TABLE DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_UPDATE AS
WITH advance AS (
     SELECT
      DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.ADVANCE_ACTIVE_USERS
 )
 ,restricted as
 (
 select DISTINCT DAVEUSERID
  FROM DAVE.LOOMIS.RESTRICTION
 )
 ,overdraft as
 (
    SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.OVERDRAFT_ACTIVE_USERS
 )
 ,legacy_user(select
    count(a.DAVEUSERID)
 FROM restricted a
 join overdraft b
 on a.DAVEUSERID = b.user_id
 left join advance c
 on b.user_id = c.user_id
 where c.user_id is null)
SELECT a.*
FROM DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT a
LEFT JOIN overdraft_user b
ON a.USER_ID = b.USER_ID
WHERE b.USER_ID IS NULL