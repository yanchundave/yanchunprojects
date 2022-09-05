DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.USER_TS_2021;
CREATE TABLE ACCOUNTING.DBT_LOCAL.USER_TS_2021 AS
WITH  user_pv AS
(
    SELECT
    USER_ID,
    TO_DATE(PV_TS) AS startdate,
    DATEADD('DAY', 14, TO_DATE(PV_TS)) AS enddate,
    PLATFORM,
    ATTRIBUTION,
    NETWORK
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_ATTRIBUTION
    WHERE TO_DATE(PV_TS) >= '2021-01-01'
    AND TO_DATE(PV_TS) <= '2022-03-01'
),
user_activity AS
(
    SELECT user_pv.USER_ID,
    au.EVENT_DS_PST AS EVENTTIME
    FROM user_pv
    LEFT JOIN
    ANALYTIC_DB.DBT_metrics.one_dave_active_users au
    WHERE user_pv.startdate <= DATE(au.EVENT_DS_PST)
    AND user_pv.enddate >= DATE(au.EVENT_DS_PST)
),
user_session as
(
    SELECT USER_ID,
    COUNT(DISTINCT EVENTTIME) AS session_total
    FROM user_activity
    GROUP BY USER_ID
),
bank_connection as (
    SELECT distinct bc.user_id,
                    CASE WHEN DISPLAY_NAME is null THEN 'blank'
                    WHEN DISPLAY_NAME like 'Chime%' THEN 'CHIME'
                    WHEN DISPLAY_NAME like 'Varo%' or DISPLAY_NAME ='Albert' or DISPLAY_NAME ='Step'   or DISPLAY_NAME like 'Go%Bank%'
                    THEN 'other neo bank'
                    ELSE 'traiditonal' END AS bank_category,
                    bc.id  as bank_account_id,
                    rank() over (partition by bc.USER_ID order by bc.UPDATED,bc.LAST_PULL, bc.INITIAL_PULL desc) as rank,
                    bc.HAS_VALID_CREDENTIALS,
                    HAS_TRANSACTIONS

    FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT ba
             LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE."BANK_CONNECTION" as  bc ON ba.user_id = bc.user_id AND ba.bank_connection_id = bc.id
             left join analytic_db.dbt_marts.fct_advance_approvals ap on ap.user_id = bc.user_id AND ap.BANK_ACCOUNT_ID=bc.id
    WHERE bc.deleted is NULL
      and ba.deleted is NULL
      AND ba._FIVETRAN_DELETED = 'false'
      AND bc._FIVETRAN_DELETED = 'false'
      AND bc.banking_data_source in ('PLAID', 'MX')
      AND (ba.MICRO_DEPOSIT IN ('COMPLETED') or ba.MICRO_DEPOSIT is NULL)
      qualify Rank = 1
),
bank_series as
(
    select user_pv.USER_ID, b.INSTITUTION_ID
    from user_pv
    left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION b
    on user_pv.USER_ID = b.USER_ID
    AND user_pv.startdate <= to_date(b.CREATED)
    AND user_pv.enddate >= to_date(b.CREATED)

),
bank_summary as
(
    select USER_ID,
    COUNT(INSTITUTION_ID) as account_total
    from bank_series
    GROUP BY USER_ID
),
advance_records AS
(
    SELECT

    user_pv.USER_ID,
    adv_fee_orig + adv_tip_orig AS revenue
    FROM user_pv
    JOIN ACCOUNTING.DBT_PROD.DIM_ADVANCE_FLAT advance
    ON user_pv.USER_ID = advance.user_id
    and TO_DATE(advance.advance_disbursement_time_utc) >= user_pv.startdate
    and TO_DATE(advance.advance_disbursement_time_utc) <= user_pv.enddate
),
overdraft_records AS
(
    SELECT
    user_pv.USER_ID AS USER_ID,
    overdraft_service_fee_orig +
    overdraft_express_fee_orig +
    overdraft_user_tip_orig AS revenue
    FROM user_pv
    JOIN ACCOUNTING.DBT_PROD.DIM_OVERDRAFT_FLAT overdraft
    ON user_pv.USER_ID = overdraft.user_id
    and TO_DATE(overdraft.overdraft_disbursement_time_utc) >= user_pv.startdate
    and TO_DATE(overdraft.overdraft_disbursement_time_utc) <= user_pv.enddate
),
revenue_summary as
(
    select USER_ID,
    SUM(revenue) as revenue
    from
    (
        select * from advance_records
        UNION
        select * from overdraft_records
    ) as a
    group by USER_ID

)
select user_pv.*,
user_session.session_total,
bank_connection.bank_category,
bank_summary.account_total,
revenue_summary.revenue
from user_pv
left join user_session
on user_pv.USER_ID = user_session.USER_ID
left join bank_connection
on user_pv.USER_ID = bank_connection.USER_ID
left join bank_summary
on user_pv.USER_ID = bank_summary.USER_ID
left join revenue_summary
on user_pv.USER_ID = revenue_summary.USER_ID;

---SESSION_FEATURE_1
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
    WHERE DATE(EVENT_DS_PST) < DATE('2022-01-01')
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
SELECT A.USER_ID, A.SESSIONTOTAL, A.MAXDATE, B.MONTHNUMBER, B.MONTHSESSION,
DATEDIFF('day', DATE(A.MAXDATE), DATE('2022-01-01')) AS LASTSESSION
FROM GROUP_METRIC A
JOIN MONTH_METRIC B
ON A.USER_ID = B.USER_ID;

-----USER SESSION FEATURE 2
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
SELECT A.USER_ID, A.LASTQUARTER_SESSION, B.SESSION_STR, C.SESSIONTOTAL, C.LASTSESSION, C.ACTIVEMONTH
FROM LASTQUARTER A
JOIN SESSIONARRAY B
ON A.USER_ID = B.USER_ID
JOIN SESSIONFEATURE C
ON B.USER_ID = C.USER_ID;