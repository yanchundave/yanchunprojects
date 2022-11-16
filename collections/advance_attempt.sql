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

-------------RESTRICTED USERS IN OVERDRAFT
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
,TMP AS (
select
a.DAVEUSERID
from restricted a
join ANALYTIC_DB.DBT_metrics.OVERDRAFT_ACTIVE_USERS b
on a.DAVEUSERID = b.USER_ID
  )

 SELECT COUNT(DISTINCT DAVEUSERID) FROM TMP

 -----Payment frequency for each advance_id
 with temp as (SELECT advance_id, count(distinct payment_id) as paymentcount
FROM DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_UPDATE
group by advance_id)
select paymentcount, count(advance_id) as advancecount, count(advance_id)/(select count(advance_id) from temp) as ratio
from temp group by paymentcount
order by paymentcount

-----SESSION

DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_SESSION;
CREATE table DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_SESSION AS
with attemp as
(
    select
   user_id,
  advance_id,
  payment_id,
  payment_amount,
  date_ts,
  attempt_id,
  outstanding,
  row_number() over (partition by advance_id order by date_ts) as rownumber
  from DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_UPDATE
)
,attemp_time as (
 select
  a.*,
  b.date_ts as pre_ts,
  a.date_ts - b.date_ts as timediff,
  case
   WHEN a.date_ts - b.date_ts > 3600000 or b.date_ts is null then 1
    else 0
  end as sessionnumber
  from attemp a
  left join attemp b
  on a.advance_id = b.advance_id
  and a.rownumber = b.rownumber + 1
)
,
attemp_session as
(
select
  a.*,
  SUM(sessionnumber) OVER (PARTITION BY advance_id ORDER BY a.date_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumsession
  from attemp_time a
)
select * from attemp_session

-------Users session with sequential payment_id
WITH TEMP AS (
SELECT ADVANCE_ID, CUMSESSION, COUNT(PAYMENT_ID) AS SESSIONTOTAL
FROM DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_SESSION
GROUP BY ADVANCE_ID, CUMSESSION
HAVING COUNT(PAYMENT_ID) > 2
)
SELECT COUNT(DISTINCT B.USER_ID)
FROM (SELECT DISTINCT ADVANCE_ID FROM TEMP) A JOIN
(SELECT USER_ID, ADVANCE_ID, COUNT(*)
FROM DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_SESSION
GROUP BY USER_ID, ADVANCE_ID) B
ON A.ADVANCE_ID = B.ADVANCE_ID

-------
with attemp as
(
    select
   user_id,
  advance_id,
  payment_id,
  payment_amount,
  date_ts,
  attempt_id,
  outstanding,
  row_number() over (partition by advance_id order by date_ts) as rownumber
  from DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_UPDATE
)
,attemp_time as (
 select
  a.*,
  b.date_ts as pre_ts,
  a.date_ts - b.date_ts as timediff,
  case
   WHEN a.date_ts - b.date_ts > 3600000 * 24 or b.date_ts is null then 1
    else 0
  end as sessionnumber
  from attemp a
  left join attemp b
  on a.advance_id = b.advance_id
  and a.rownumber = b.rownumber + 1
)
,attemp_session as
(
select
  a.*,
  SUM(sessionnumber) OVER (PARTITION BY advance_id ORDER BY a.date_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumsession
  from attemp_time a
)
,TEMP AS (
SELECT ADVANCE_ID, CUMSESSION, COUNT(PAYMENT_ID) AS SESSIONTOTAL
FROM DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_SESSION
GROUP BY ADVANCE_ID, CUMSESSION
HAVING COUNT(PAYMENT_ID) > 2
)
SELECT COUNT(DISTINCT B.USER_ID)
FROM (SELECT DISTINCT ADVANCE_ID FROM TEMP) A JOIN
(SELECT USER_ID, ADVANCE_ID, COUNT(*)
FROM DBT.DEV_YANCHUN_PUBLIC.ADVANCE_ATTEMPT_SESSION
GROUP BY USER_ID, ADVANCE_ID) B
ON A.ADVANCE_ID = B.ADVANCE_ID

----Restricted & (active advance | active overdraft)
with active_user as
(
    select distinct user_id from ANALYTIC_DB.DBT_METRICS.OVERDRAFT_ACTIVE_USERS
  UNION
  SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.ADVANCE_ACTIVE_USERS
)
,restricted as
(
    select DISTINCT DAVEUSERID
  FROM DAVE.LOOMIS.RESTRICTION
)
SELECT COUNT(distinct b.DAVEUSERID)
FROM (select distinct user_id from active_user) a
join restricted b
on a.user_id = b.DAVEUSERID

-----all kinds of users
with active_user as
(
    select distinct user_id from ANALYTIC_DB.DBT_METRICS.OVERDRAFT_ACTIVE_USERS
  UNION
  SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.ADVANCE_ACTIVE_USERS
)
,restricted as
(
    select DISTINCT DAVEUSERID
  FROM DAVE.LOOMIS.RESTRICTION
)
SELECT COUNT(distinct b.DAVEUSERID)
FROM (select distinct user_id from active_user) a
join restricted b
on a.user_id = b.DAVEUSERID

select count(DAVEUSERID) from (select DISTINCT DAVEUSERID
  FROM DAVE.LOOMIS.RESTRICTION)

 select count(user_id) from (
 SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.ADVANCE_ACTIVE_USERS
 )

  select count(user_id) from (
 SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.OVERDRAFT_ACTIVE_USERS
 )

 with overlap as (
 select distinct a.user_id
 from ( SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.ADVANCE_ACTIVE_USERS) a
 join (SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.OVERDRAFT_ACTIVE_USERS) b
 on a.user_id = b.user_id)
 ,restricted as
 (
 select DISTINCT DAVEUSERID
  FROM DAVE.LOOMIS.RESTRICTION
 )
 select count(distinct DAVEUSERID)
 from overlap a join restricted b
 on a.user_id = b.DAVEUSERID

 with advance as
 (
     SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.ADVANCE_ACTIVE_USERS
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
 select
    count(a.DAVEUSERID)
 FROM restricted a
 join advance b
 on a.DAVEUSERID = b.user_id
 left join overdraft c
 on b.user_id = c.user_id
 where c.user_id is null


 with advance as
 (
     SELECT DISTINCT user_id FROM ANALYTIC_DB.DBT_METRICS.ADVANCE_ACTIVE_USERS
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
 select
    count(a.DAVEUSERID)
 FROM restricted a
 join overdraft b
 on a.DAVEUSERID = b.user_id
 left join advance c
 on b.user_id = c.user_id
 where c.user_id is null