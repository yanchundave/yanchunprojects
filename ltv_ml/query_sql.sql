-----User Last Event before 2022
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_START_RECENT;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_START_RECENT AS
with user_pv as
(
   SELECT USER_ID, to_date(PV_TS) as starttime
    FROM ANALYTIC_DB.DBT_marts.new_user_reattribution
    WHERE to_date(PV_TS) >= '2021-01-01'
    AND to_date(PV_TS) < '2022-01-01'

),
user_event as
(
    SELECT USER_ID, MAX(EVENT_DS_PST) AS last_date
    FROM ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS
    WHERE date(EVENT_DS_PST) < date('2022-01-01')
    GROUP BY USER_ID
)
SELECT user_pv.USER_ID AS USER_ID, user_pv.starttime AS STARTTIME, COALESCE(user_event.last_date, user_pv.starttime) AS LATETIME
FROM user_pv LEFT JOIN user_event
ON user_pv.USER_ID = user_event.USER_ID;

----TEST START RECENCY TABLE
SELECT
COUNT(USER_ID) AS NOTZEROS,
SUM(CASE WHEN LATETIME IS NULL THEN 1
ELSE 0 END) AS ZEROS
FROM DBT.DEV_YANCHUN_PUBLIC.USER_START_RECENT;

-----User final event

DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_A;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_A
AS
with users as (
select USER_ID, STARTTIME, LATETIME
from DBT.DEV_YANCHUN_PUBLIC.USER_START_RECENT
where LATETIME IS NOT NULL
  AND LATETIME >= '2021-01-01'
  AND LATETIME < '2021-07-01'
)
SELECT A.USER_ID, A.STARTTIME AS STARTTIME, A.LATETIME AS LATETIME, B.EVENT_TYPE, B.CLIENT_EVENT_TIME
FROM users as A
JOIN
ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO B
ON to_varchar(A.USER_ID) = to_varchar(B.USER_ID)
AND to_date(A.LATETIME) = to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz))
WHERE to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz)) >='2021-01-01'
AND to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz)) < '2021-07-01'
AND B.SESSION_ID > 0
AND B.EVENT_TYPE NOT LIKE '[%'
ORDER BY B.CLIENT_EVENT_TIME;

DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_B;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_B
AS
with users as (
select USER_ID, STARTTIME, LATETIME
from DBT.DEV_YANCHUN_PUBLIC.USER_START_RECENT
where LATETIME IS NOT NULL
  AND LATETIME >= '2021-07-01'
  AND LATETIME < '2022-01-01'
)
SELECT A.USER_ID, A.STARTTIME AS STARTTIME, A.LATETIME AS LATETIME,
B.EVENT_TYPE, B.CLIENT_EVENT_TIME,
ROW_NUMBER() OVER (PARTITION BY A.USER_ID ORDER BY B.CLIENT_EVENT_TIME) AS ROW_NUMBER
FROM users as A
JOIN
ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO B
ON to_varchar(A.USER_ID) = to_varchar(B.USER_ID)
AND to_date(A.LATETIME) = to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz))
WHERE to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz)) >='2021-07-01'
AND to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz)) < '2022-01-01'
AND B.SESSION_ID > 0
AND B.EVENT_TYPE NOT LIKE '[%'
ORDER BY B.CLIENT_EVENT_TIME;

---USER LAST EVEN STRING
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_STR;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_STR AS
with event_a as
(
select USER_ID, STARTTIME, ARRAY_TO_STRING(ARRAY_AGG(EVENT_TYPE), ',') as eventstring
  FROM DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_A
  GROUP BY USER_ID, STARTTIME
),
event_b as
(
SELECT USER_ID, STARTTIME, ARRAY_TO_STRING(ARRAY_AGG(EVENT_TYPE), ',') as eventstring
  FROM DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_B
  GROUP BY USER_ID, STARTTIME
)
SELECT * FROM event_a
UNION
SELECT * FROM event_b;

----USER REVENUE 2022
DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
AS
with user_pv AS
(
    SELECT
    USER_ID,
    TO_DATE(PV_TS) AS startdate
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(PV_TS) >= DATE('2021-01-01')
    AND to_date(PV_TS) < DATE('2022-01-01')
),
advance_records AS
(
    SELECT
    TO_VARCHAR(advance_id) AS trans_id,
    user_pv.USER_ID AS user_id,
    user_pv.startdate AS startdate,
    TO_DATE(advance_disbursement_time_utc) AS trans_time,
    adv_fee_orig + adv_tip_orig AS revenue
    FROM DBT.DEV_SOLIN_FINANCE.dim_advance_flat advance
    JOIN user_pv
    ON user_pv.USER_ID = advance.user_id
    WHERE TO_DATE(advance_disbursement_time_utc) < DATE('2022-07-01')
),
overdraft_records AS
(
    SELECT
    overdraft_id AS trans_id,
    user_pv.USER_ID AS user_id,
    user_pv.startdate AS startdate,
    TO_DATE(overdraft_disbursement_time_utc) AS trans_time,
    overdraft_service_fee_orig +
    overdraft_express_fee_orig +
    overdraft_user_tip_orig AS revenue
    FROM DBT.DEV_SOLIN_FINANCE.dim_overdraft_flat overdraft
    JOIN user_pv
    ON user_pv.USER_ID = overdraft.user_id
    WHERE TO_DATE(overdraft_disbursement_time_utc) < DATE('2022-07-01')
)
SELECT * FROM advance_records
UNION ALL
SELECT * FROM overdraft_records;

----USER ACTIVITY
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_MONTH_ACTIVE;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_MONTH_ACTIVE AS
with USER_PV as
(
   SELECT USER_ID, to_date(PV_TS) as STARTTIME
    FROM ANALYTIC_DB.DBT_marts.new_user_reattribution
    WHERE to_date(PV_TS) >= '2021-01-01'
    AND to_date(PV_TS) < '2022-01-01'

)
SELECT A.USER_ID, LEFT(A.STARTTIME, 7) AS STARTMONTH, LEFT(B.ONE_DAVE_ACTIVE_DS, 7) AS MONTH, COUNT(B.ONE_DAVE_ACTIVE_DS) AS ACTIVENUM
FROM USER_PV A
LEFT JOIN DBT.DBT_METRICS.ONE_DAVE_ACTIVE_USERS B
ON A.USER_ID = B.USER_ID
WHERE date(B.ONE_DAVE_ACTIVE_DS) >= date('2021-01-01')
AND date(B.ONE_DAVE_ACTIVE_DS) < date('2022-01-01')
GROUP BY A.USER_ID, LEFT(A.STARTTIME, 7), LEFT(B.ONE_DAVE_ACTIVE_DS, 7);

----USER REVENUE
DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_MONTH_REVENUE;
CREATE TABLE DBT.DEV_YANCHUN_PUBLIC.USER_MONTH_REVENUE
AS
SELECT USER_ID, LEFT(STARTDATE, 7) AS STARTMONTH, LEFT(TRANS_TIME, 7) AS MONTH, SUM(REVENUE) AS REVENUESUM
FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
GROUP BY USER_ID, LEFT(STARTDATE, 7), LEFT(TRANS_TIME, 7);

--- User demographic property

DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_DEMOGRAPHICS;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_DEMOGRAPHICS AS
SELECT USER_ID, BOD_ACCOUNT_OPEN_USER, BOD_ACCOUNT_FUNDED_USER, BOD_ACCOUNT_ACTIVATED_USER,
CASE WHEN PLATFORM LIKE '%ios%' THEN 1 ELSE 0 END AS IOS,
CASE WHEN PLATFORM LIKE '%android%' THEN 1 ELSE 0 END AS ANDROID
FROM ANALYTIC_DB.DBT_marts.new_user_reattribution
WHERE to_date(PV_TS) >= '2021-01-01'
AND to_date(PV_TS) < '2022-01-01'

-----User avereage advance amount
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_ADVANCE_2022;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_ADVANCE_2022
AS
with user_pv AS
(
    SELECT
    USER_ID,
    TO_DATE(PV_TS) AS startdate
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(PV_TS) >= DATE('2021-01-01')
    AND to_date(PV_TS) < DATE('2022-01-01')
),
advance_records AS
(
    SELECT
    user_pv.USER_ID AS user_id,
    user_pv.startdate AS startdate,
    LEFT(advance_disbursement_time_utc, 7) AS trans_time,
    AVG(ADV_AMOUNT) AS advance_amount
    FROM DBT.DEV_SOLIN_FINANCE.dim_advance_flat advance
    JOIN user_pv
    ON user_pv.USER_ID = advance.user_id
    GROUP BY user_pv.USER_ID, LEFT(advance_disbursement_time_utc, 7), startdate
),
overdraft_records AS
(
    SELECT
    user_pv.USER_ID AS user_id,
    user_pv.startdate AS startdate,
    LEFT(overdraft_disbursement_time_utc, 7) AS trans_time,
    AVG(OVERDRAFT_DISBURSEMENT_AMT) AS advance_amount
    FROM DBT.DEV_SOLIN_FINANCE.dim_overdraft_flat overdraft
    JOIN user_pv
    ON user_pv.USER_ID = overdraft.user_id
    GROUP BY user_pv.USER_ID, LEFT(overdraft_disbursement_time_utc, 7), startdate
)
SELECT * FROM advance_records
UNION ALL
SELECT * FROM overdraft_records;

----USER_SEQ
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_SEQ_DEC;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_SEQ_DEC
AS
WITH TABLE_A AS (
SELECT USER_ID, STARTTIME, EVENT_TYPE, ROW_NUMBER, CLIENT_EVENT_TIME
  FROM DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_B
  WHERE STARTTIME = '2021-12'
),
TABLE_B AS
(
SELECT USER_ID, STARTTIME, EVENT_TYPE, ROW_NUMBER
  FROM DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_B
  WHERE STARTTIME = '2021-12'
)
SELECT A.USER_ID, A.EVENT_TYPE, B.EVENT_TYPE, A.CLIENT_EVENT_TIME
FROM TABLE_A A
JOIN TABLE_B B
ON A.USER_ID = B.USER_ID AND A.STARTTIME = B.STARTTIME AND A.ROW_NUMBER + 1 = B.ROW_NUMBER;

---EVENT SEQ
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.EVENT_SEQ;
CREATE table DBT.DEV_YANCHUN_PUBLIC.EVENT_SEQ
AS
WITH TMP
AS
(SELECT FRONT_EVENT, BACK_EVENT, USER_ID
FROM DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_SEQ
ORDER BY FRONT_EVENT, BACK_EVENT, CLIENT_EVENT_TIME
),
EVENTLIST AS (
SELECT FRONT_EVENT, BACK_EVENT
FROM TMP
WHERE FRONT_EVENT != BACK_EVENT
GROUP BY FRONT_EVENT, BACK_EVENT
HAVING COUNT(USER_ID) <= 1130000 AND COUNT(USER_ID) > 1
ORDER BY COUNT(USER_ID) DESC
  )
  SELECT A.FRONT_EVENT, A.BACK_EVENT, ARRAY_TO_STRING(ARRAY_AGG(B.USER_ID), ',') AS USERSTR
  FROM EVENTLIST A
  JOIN DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_SEQ B
  ON A.FRONT_EVENT = B.FRONT_EVENT AND A.BACK_EVENT = B.BACK_EVENT
  GROUP BY A.FRONT_EVENT, A.BACK_EVENT

-------USER SESSION FEATURE
DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_1;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_1 AS
WITH USERLIST AS
(
    SELECT USER_ID FROM DBT.DEV_YANCHUN_PUBLIC.USER_ADVANCE_FEATURE_1
),
USERSESSION AS
(
    SELECT A.USER_ID, EVENT_DS_PST
    FROM USERLIST A
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS B
    ON A.USER_ID = B.USER_ID
),
GROUP_METRIC AS (
  SELECT USER_ID, COUNT(EVENT_DS_PST) AS SESSIONTOTAL,MAX(EVENT_DS_PST) AS MAXDATE
  FROM USERSESSION
  GROUP BY USER_ID
  ),
MONTH_METRIC AS
(
    SELECT USER_ID, LEFT(EVENT_DS_PST, 7) AS MONTHNUMBER, COUNT(EVENT_DS_PST) AS MONTHSESSION
    FROM USERSESSION
    GROUP BY USER_ID, LEFT(EVENT_DS_PST,7)
)
SELECT A.USER_ID, A.SESSIONTOTAL, A.MAXDATE, B.MONTHNUMBER, B.MONTHSESSION, DATEDIFF('day', DATE(A.MAXDATE), DATE('2022-01-01')) AS LASTSESSION
FROM GROUP_METRIC A
JOIN MONTH_METRIC B
ON A.USER_ID = B.USER_ID

----USER SESSION FEATURE 2
DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_2;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_2 AS
WITH LASTQUARTER AS (
SELECT USER_ID, SUM(MONTHSESSION) AS LASTQUARTER_SESSION
  FROM DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_1
  WHERE MONTHNUMBER IN ('2021-10', '2021-11', '2021-12')
  GROUP BY USER_ID
),
SESSIONARRAY AS
(
 SELECT USER_ID, ARRAY_TO_STRING(ARRAY_AGG(MONTHSESSION), ',') AS SESSION_STR
  FROM DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_1
  GROUP BY USER_ID
),
SESSIONFEATURE AS
(
SELECT USER_ID, SESSIONTOTAL, LASTSESSION, COUNT(MONTHNUMBER) AS ACTIVEMONTH
  FROM DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_1
  GROUP BY USER_ID, SESSIONTOTAL, LASTSESSION
)
SELECT B.USER_ID, A.LASTQUARTER_SESSION, B.SESSION_STR, C.SESSIONTOTAL, C.LASTSESSION, C.ACTIVEMONTH
FROM SESSIONARRAY B
LEFT JOIN LASTQUARTER A
ON A.USER_ID = B.USER_ID
LEFT JOIN SESSIONFEATURE C
ON B.USER_ID = C.USER_ID;

---USER CNN EVENT COUNT WITH 7 DAYS
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.CNN_EVENT;
CREATE table DBT.DEV_YANCHUN_PUBLIC.CNN_EVENT AS
WITH userlist AS
(
select USER_ID,
  to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) as event_window_start,
 DATEADD('DAY', 7, to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))) as event_window_end
from ANALYTIC_DB.DBT_MARTS.NEW_USER_ATTRIBUTION
where to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='2021-12-01'
AND to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) < '2021-12-04'
AND DELETED_STATUS = 'NOT DELETED'
AND FIRST_ADVANCE_TIMESTAMP IS NOT NULL
),
events as (
SELECT A.USER_ID AS USER_ID, A.event_window_start AS start_date, B.EVENT_TYPE as event_type,
  to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz)) AS event_date
FROM userlist as A
JOIN
ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO B
ON to_varchar(A.USER_ID) = to_varchar(B.USER_ID)
AND to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz)) <= A.event_window_end
AND to_date(convert_timezone('UTC','America/Los_Angeles',B.CLIENT_EVENT_TIME::timestamp_ntz)) >= A.event_window_start
)
select USER_ID, start_date, event_date, event_type, count(event_type) as event_volume
FROM events
GROUP BY USER_ID, start_date, event_date, event_type
order by USER_ID, start_date, event_type, event_date;

---USER SELECT REVENUE CNN
with user_revenue as
(
select USER_ID, SUM(REVENUE) as total_revenue
  from DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
  WHERE date(TRANS_TIME) >= date('2022-01-01') and date(TRANS_TIME) < DATE('2022-07-01')
  group by USER_ID
),
selected_user AS
(
SELECT USER_ID, COUNT(*) AS total
  from DBT.DEV_YANCHUN_PUBLIC.CNN_EVENT
  group by USER_ID
)
SELECT a.USER_ID, b.total_revenue
FROM selected_user a
join user_revenue b
on a.USER_ID = b.USER_ID

-----USER ADVANCE FEATURE
-----ADVANCE FEATURES
DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_ADVANCE_FEATURE_1;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_ADVANCE_FEATURE_1
AS
with USER_TRANS AS
    (
        SELECT
        USER_ID,
        STARTDATE,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
        FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
        WHERE to_date(TRANS_TIME) < '2022-01-01'
    )
    SELECT
    USER_ID,
    STARTDATE,
    COUNT(TRANS_ID) AS frequency,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('2022-01-01')) AS T,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS recency,
    DATEDIFF('day', DATE(MIN(STARTDATE)), date('2022-12-31')) AS AGE,
    AVG(REVENUE) AS monetary
    FROM USER_TRANS
    GROUP BY USER_ID, STARTDATE;

-----Account Database

SELECT * FROM ACCOUNTING.DBT_LOCAL.USER_DATA_2022

SELECT * FROM ACCOUNTING.DBT_LOCAL.USER_REV

SELECT * FROM ACCOUNTING.DBT_PROD.DIM_OVERDRAFT_FLAT --REAPLACE SONG'S OVERDRAFT TABLE

SELECT * FROM ACCOUNTING.DBT_PROD.DIM_ADVANCE_FLAT --REPLACE SONG'S ADVANCE TABLE


-----BANK CONNECTION
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION;
CREATE TABLE DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION AS
SELECT
DISTINCT bc.user_id,
CASE
  WHEN DISPLAY_NAME is null THEN 'blank'
  WHEN DISPLAY_NAME like 'Chime%' THEN 'CHIME'
  WHEN DISPLAY_NAME like 'Varo%' or DISPLAY_NAME ='Albert' or DISPLAY_NAME ='Step'   or DISPLAY_NAME like 'Go%Bank%'
  THEN 'other neo bank'
  ELSE 'traiditonal'
  END AS bank_category,
bc.id  AS bank_account_id,
RANK() OVER (PARTITION BY bc.USER_ID ORDER BY bc.UPDATED,bc.LAST_PULL, bc.INITIAL_PULL DESC) AS rank,
bc.HAS_VALID_CREDENTIALS,
HAS_TRANSACTIONS
FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT ba
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE."BANK_CONNECTION" AS  bc
ON ba.user_id = bc.user_id AND ba.bank_connection_id = bc.id
           --  left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.INSTITUTION i on i.ID = ba.INSTITUTION_ID
LEFT JOIN analytic_db.dbt_marts.fct_advance_approvals ap
ON ap.user_id = bc.user_id AND ap.BANK_ACCOUNT_ID=bc.id
WHERE 1=1
AND bc.deleted IS NULL
AND ba.deleted IS NULL
AND ba._FIVETRAN_DELETED = 'false'
AND bc._FIVETRAN_DELETED = 'false'
AND bc.banking_data_source in ('PLAID', 'MX')
--     and bc.User_ID in ('1806882','1393948','11609725')
AND (ba.MICRO_DEPOSIT IN ('COMPLETED') or ba.MICRO_DEPOSIT is NULL)
qualify Rank = 1

-------
FORECAST_DATE = '2021-04-01'

DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022;
CREATE table DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022
AS
SELECT
USER_ID,
TO_DATE(PV_TS) AS startdate,
PLATFORM,
ATTRIBUTION,
NETWORK,
BOD_ACCOUNT_OPEN_USER,
BOD_DIRECT_DEPOSIT_USER
FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
WHERE to_date(PV_TS) >= DATE('2021-01-01')
AND to_date(PV_TS) < DATE('2022-09-01');
------- NEW USER YELLOW FEATURE

DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022;
CREATE table DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022
AS
WITH USER_SUBSET AS
(
  SELECT
  USER_ID,
  STARTDATE,
  PLATFORM,
  ATTRIBUTION,
  NETWORK,
  BOD_ACCOUNT_OPEN_USER,
  BOD_DIRECT_DEPOSIT_USER,
  DATE({FORECAST_DATE}) AS FORECASTDATE,
  DATEADD('month', -3, {FORECAST_DATE}) AS STARTDATE,
  DATEADD('month', 6, {FORECAST_DATE}) AS ENDDATE
  FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
  WHERE STARTDATE < {FORECAST_DATE}
),
UW AS (
  SELECT
  dau.user_id,
  row_number() over (partition by dau.USER_ID order by dau.requested_ds_pst DESC ) rank,
  dau.requested_ds_pst closest_request,
  dau.disbursement_ds_pst  closest_dis,
  case when closest_dis is null then 1 else 0 end as MOST_RECENT_REQUEST_DECLINE,
  dau.payback_ds_pst closest_expect_payback,
  dau.prev_advance_bucket closest_prev_advance_bucket,
  dau.max_approved_amount,
  lag(max_approved_amount)over (partition by dau.user_id order by dau.requested_ds_pst DESC  ) AS LAST_MAX_APPROVED_AMOUNT,
  COALESCE(dau.advance_taken_amount, dau.overdraft_amount) AS ADVANCE_TAKEN_AMOUNT,
  FROM ANALYTIC_DB.DBT_marts.dim_advance_users as dau
  JOIN USER_SUBSET USERS
  ON dau.user_id=USERS.USER_ID
  where REQUESTED_DS_PST <USERS.FORECASTDATE
  qualify rank = 1
),
APPROVED_BANK AS (
  SELECT advance_requests.user_id,
  COUNT(DISTINCT
    IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL,
    advance_requests.bank_account_id, NULL)) AS approved_bank_count,
  FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
  LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
  ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
  LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
  ON advance_requests.advance_approval_id = o2_takeout.approval_id
  LEFT JOIN USER_SUBSET USERS
  ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
  where ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
  group by 1
  )
SELECT
USERS.USER_ID,
USERS.STARTDATE,
USERS.PLATFORM,
USERS.ATTRIBUTION,
USERS.NETWORK,
USERS.BOD_ACCOUNT_OPEN_USER,
USERS.BOD_DIRECT_DEPOSIT_USER,
USERS.FORECASTDATE,
USERS.STARTDATE,
USERS.ENDDATE,
BANKING.BANK_CATEGORY,
BANKING.HAS_VALID_CREDENTIALS,
UW.MOST_RECENT_REQUEST_DECLINE,
UW.LAST_MAX_APPROVED_AMOUNT,
UW.ADVANCE_TAKEN_AMOUNT,
FROM USER_SUBSET USERS
LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
ON USERS.USER_ID
LEFT JOIN UW
ON USERS.USER_ID = UW.USER_ID
LEFT JOIN APPROVED_BANK
ON USERS.USER_ID = APPROVED_BANK.USER_ID;



-----user_transaction_2022

DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022;
CREATE table ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022
AS
with user_pv AS
(
    SELECT
    USER_ID,
    TO_DATE(PV_TS) AS startdate
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(PV_TS) >= DATE('2021-01-01')

),
advance_records AS
(
    SELECT
    TO_VARCHAR(advance_id) AS trans_id,
    user_pv.USER_ID AS user_id,
    user_pv.startdate AS startdate,
    TO_DATE(advance_disbursement_time_utc) AS trans_time,
    adv_fee_orig + adv_tip_orig AS revenue
    FROM user_pv
    LEFT JOIN ACCOUNTING.DBT_PROD.DIM_ADVANCE_FLAT advance
    ON user_pv.USER_ID = advance.user_id
),
overdraft_records AS
(
    SELECT
    overdraft_id AS trans_id,
    user_pv.USER_ID AS user_id,
    user_pv.startdate AS startdate,
    TO_DATE(overdraft_disbursement_time_utc) AS trans_time,
    overdraft_service_fee_orig +
    overdraft_express_fee_orig +
    overdraft_user_tip_orig AS revenue
    FROM user_pv
    LEFT JOIN ACCOUNTING.DBT_PROD.DIM_OVERDRAFT_FLAT overdraft
    ON user_pv.USER_ID = overdraft.user_id
)
SELECT * FROM advance_records
UNION ALL
SELECT * FROM overdraft_records;

------User happened revenue
with USERS as
 (
   select USER_ID FROM
 DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 WHERE STARTDATE >= DATE('2022-06-01') AND STARTDATE < DATE('2022-09-01')
 )
 ,trans as
 (
   SELECT users.USER_ID, REVENUE FROM
   USERS LEFT JOIN
 ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
   ON USERS.USER_ID = TRANS.USER_ID
 )
 SELECT SUM(REVENUE)/COUNT(DISTINCT USER_ID) FROM trans;

