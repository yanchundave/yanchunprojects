DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.LTV_USER_2022;
CREATE table ACCOUNTING.DBT_LOCAL.LTV_USER_2022
AS
SELECT
USER_ID,
TO_DATE(PV_TS) AS startdate,
PLATFORM,
ATTRIBUTION,
NETWORK
FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
WHERE to_date(PV_TS) >= DATE('2021-01-01')
AND to_date(PV_TS) < DATE('2022-08-01');


----- CREATE TABLE
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
    AND to_date(PV_TS) < DATE('2022-08-01')
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

----- EXTRACT TRAINING DATASET
DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022_TRAINING;
CREATE table ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022_TRAINING AS
with
USER AS
    (
        SELECT USER_ID FROM ACCOUNTING.DBT_LOCAL.LTV_USER_2022 WHERE startdate < date('2021-10-01')
    ),
USER_TRANS AS
    (
        SELECT
        USER.USER_ID AS USER_ID,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
        FROM USER
        LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
        ON USER.USER_ID = TRANS.USER_ID
        WHERE TRANS_TIME IS NOT NULL AND to_date(TRANS_TIME) < '2022-01-01'
    )
    SELECT
    USER_ID,
    DATE(MIN(TRANS_TIME)) AS first_trans,
    COUNT(TRANS_ID) - 1 AS frequency,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('2022-01-01')) AS T,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS recency,
    AVG(REVENUE) AS monetary
    FROM USER_TRANS
    GROUP BY USER_ID;

---- Extract test dataset
DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022_TEST;
CREATE table ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022_TEST AS
WITH
USER AS
(
   SELECT USER_ID FROM ACCOUNTING.DBT_LOCAL.LTV_USER_2022 WHERE startdate < date('2021-10-01')
)
SELECT USER.USER_ID AS USER_ID, COUNT(TRANS_ID) AS trans_num, SUM(REVENUE) AS real_revenue
FROM  USER
LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
ON USER.USER_ID = TRANS.USER_ID
WHERE TRANS_TIME IS NOT NULL AND to_date(TRANS_TIME) >= date('2022-01-01')
AND to_date(TRANS_TIME) < date('2022-07-01')
GROUP BY USER.USER_ID


---------LTV TS
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
    AND TO_DATE(PV_TS) <= '2022-08-22'
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


-------------sql query for analysis and metrics
--Forecast
WITH PREDICT AS
(SELECT USERID, T_VALUE, TOTALREVENUE,PROB_ALIVE,
CASE
WHEN PRED_NUM < 2 THEN 1
ELSE 0
END AS CHURN,
START_MONTH
FROM ANALYTIC_DB.MODEL_OUTPUT.STATISTICAL_FORECAST_RESULT
WHERE PREDICT_LABEL = 1)
SELECT AVG(T_VALUE) AS ARPU, SUM(CHURN)/COUNT(DISTINCT USERID) AS CHURN_RATE,
(AVG(T_VALUE) * COUNT(DISTINCT USERID) / SUM(CHURN) + SUM(TOTALREVENUE) / COUNT(DISTINCT USERID)) AS LTV,
COUNT(DISTINCT USERID) AS ACQUIRED_USER,
SUM(TOTALREVENUE) / COUNT(DISTINCT USERID) as AVG_REAL,
(AVG(T_VALUE) / (1- AVG(PROB_ALIVE)) + SUM(TOTALREVENUE) / COUNT(DISTINCT USERID)) AS LTV_UPPER,
1-AVG(PROB_ALIVE) AS CHURN_UPPER
FROM PREDICT
;
--Test Dataset
WITH PREDICT AS
(SELECT USERID, T_VALUE, REAL_REVENUE,PROB_ALIVE,
CASE
WHEN PRED_NUM < 2 THEN 1
ELSE 0
END AS CHURN,
START_MONTH
FROM ANALYTIC_DB.MODEL_OUTPUT.STATISTICAL_TRAINING_RESULT
WHERE PREDICT_LABEL = 1
AND START_MONTH < '2021-10-01')
SELECT AVG(T_VALUE) AS ARPU, SUM(CHURN)/COUNT(DISTINCT USERID) AS CHURN_RATE,
(AVG(T_VALUE) * COUNT(DISTINCT USERID) / SUM(CHURN) + SUM(REAL_REVENUE) / COUNT(DISTINCT USERID)) AS LTV,
COUNT(DISTINCT USERID) AS ACQUIRED_USER,
SUM(REAL_REVENUE) / COUNT(DISTINCT USERID) as AVG_REAL,
(AVG(T_VALUE) / (1- AVG(PROB_ALIVE)) + SUM(REAL_REVENUE) / COUNT(DISTINCT USERID)) AS LTV_UPPER,
1-AVG(PROB_ALIVE) AS CHURN_UPPER
FROM PREDICT