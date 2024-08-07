DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.LTV_LONG_INPUT;
CREATE TABLE ACCOUNTING.DBT_LOCAL.LTV_LONG_INPUT
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
        DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE())) AS FORECASTDATE
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
    WHERE STARTDATE < DATEADD('month', -9, DATE_TRUNC('month', CURRENT_DATE()))
),
UW AS
(
    SELECT
        dau.USER_ID,
        ROW_NUMBER() OVER (PARTITION BY dau.USER_ID ORDER BY dau.REQUESTED_DS_PST DESC ) RANK,
        dau.REQUESTED_DS_PST AS CLOSEST_REQUEST,
        dau.DISBURSEMENT_DS_PST AS CLOSEST_DIS,
        CASE WHEN CLOSEST_DIS IS NULL THEN 1 ELSE 0 END AS MOST_RECENT_REQUEST_DECLINE,
        dau.PAYBACK_DS_PST AS CLOSEST_EXPECT_PAYBACK,
        dau.PREV_ADVANCE_BUCKET AS CLOSEST_PREV_ADVANCE_BUCKET,
        dau.MAX_APPROVED_AMOUNT,
        LAG(MAX_APPROVED_AMOUNT) OVER (PARTITION BY dau.USER_ID ORDER BY dau.REQUESTED_DS_PST DESC ) AS LAST_MAX_APPROVED_AMOUNT,
        COALESCE(dau.ADVANCE_TAKEN_AMOUNT, dau.OVERDRAFT_AMOUNT) AS ADVANCE_TAKEN_AMOUNT
    FROM ANALYTIC_DB.DBT_marts.dim_advance_users as dau
    JOIN USER_SUBSET USERS
    ON dau.USER_ID=USERS.USER_ID
    WHERE REQUESTED_DS_PST <USERS.FORECASTDATE
    QUALIFY RANK = 1
),
APPROVED_BANK AS (
    SELECT
        advance_requests.USER_ID,
        COUNT(DISTINCT
            IFF(TO_NUMERIC(advance_requests.MAX_APPROVED_AMOUNT) IS NOT NULL,
            advance_requests.BANK_ACCOUNT_ID, NULL)) AS APPROVED_BANK_COUNT
    FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
    ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
    ON advance_requests.advance_approval_id = o2_takeout.approval_id
    LEFT JOIN USER_SUBSET USERS
    ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
    WHERE ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
    GROUP BY 1
),
USER_TRANS AS
(
    SELECT
        USERS.USER_ID,
        USERS.FORECASTDATE,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
    FROM USER_SUBSET USERS
    JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE TO_DATE(TRANS_TIME) <USERS.FORECASTDATE
),
RFM AS
(
    SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), MAX(FORECASTDATE)) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY,
        ARRAY_TO_STRING(ARRAY_AGG(DATEDIFF('DAY', DATE(TRANS_TIME), FORECASTDATE)), ',') AS TRANS_LIST,
        ARRAY_TO_STRING(ARRAY_AGG(REVENUE), ',') AS MONETARY_LIST
    FROM USER_TRANS
    GROUP BY USER_ID
),
USERSESSIONMONTHLY AS
(
    SELECT
        USERS.USER_ID,
        DATE_TRUNC('month', ACTIVE.EVENT_DS_PST) AS MONTH,
        COUNT(ACTIVE.EVENT_DS_PST) AS MONTH_ACTIVE
    FROM USER_SUBSET USERS
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
    ON USERS.USER_ID = ACTIVE.USER_ID
    WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
    AND DATE(ACTIVE.EVENT_DS_PST) >= DATEADD('month', -6, USERS.FORECASTDATE)
    GROUP BY 1, 2
),
USERSESSION AS
(
    SELECT
        USER_ID,
        ARRAY_TO_STRING(ARRAY_AGG(MONTH_ACTIVE), ',') AS SESSION_LIST,
        SUM(MONTH_ACTIVE) AS SESSIONTOTAL
    FROM USERSESSIONMONTHLY
    GROUP BY USER_ID
),
UR AS (
    SELECT
        USERS.USER_ID,
        SUM(REVENUE) AS REALREVENUE
    FROM USER_SUBSET USERS
    LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE DATE(TRANS.TRANS_TIME) >= USERS.FORECASTDATE AND DATE(TRANS.TRANS_TIME) < DATEADD('month', 6, USERS.FORECASTDATE)
    GROUP BY USERS.USER_ID
),
TRAINING AS
(
    SELECT
        USERS.USER_ID,
        USERS.STARTDATE,
        USERS.PLATFORM,
        USERS.ATTRIBUTION,
        USERS.NETWORK,
        USERS.BOD_ACCOUNT_OPEN_USER,
        USERS.BOD_DIRECT_DEPOSIT_USER,
        USERS.FORECASTDATE,
        BANKING.BANK_CATEGORY,
        BANKING.HAS_VALID_CREDENTIALS,
        UW.MOST_RECENT_REQUEST_DECLINE,
        UW.LAST_MAX_APPROVED_AMOUNT,
        UW.ADVANCE_TAKEN_AMOUNT,
        AB.APPROVED_BANK_COUNT,
        RFM.FIRST_TRANS,
        RFM.FREQUENCY,
        RFM.T,
        RFM.RECENCY,
        RFM.MONETARY,
        RFM.TRANS_LIST,
        RFM.MONETARY_LIST,
        SQ.SESSIONTOTAL,
        SQ.SESSION_LIST,
        UR.REALREVENUE
    FROM USER_SUBSET USERS
    LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
    ON USERS.USER_ID = BANKING.USER_ID
    LEFT JOIN UW
    ON USERS.USER_ID = UW.USER_ID
    LEFT JOIN APPROVED_BANK AS AB
    ON USERS.USER_ID = AB.USER_ID
    LEFT JOIN RFM
    ON USERS.USER_ID = RFM.USER_ID
    LEFT JOIN USERSESSION SQ
    ON USERS.USER_ID = SQ.USER_ID
    LEFT JOIN UR
    ON USERS.USER_ID = UR.USER_ID
),
USER_SUBSET_FORECAST AS
(
    SELECT
        USER_ID,
        STARTDATE,
        PLATFORM,
        ATTRIBUTION,
        NETWORK,
        BOD_ACCOUNT_OPEN_USER,
        BOD_DIRECT_DEPOSIT_USER,
        DATE_TRUNC('month', CURRENT_DATE())AS FORECASTDATE
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
    WHERE STARTDATE < DATEADD('month', -3, DATE_TRUNC('month', CURRENT_DATE()))
),
UW_FORECAST AS
(
    SELECT
        dau.USER_ID,
        ROW_NUMBER() OVER (PARTITION BY dau.USER_ID ORDER BY dau.REQUESTED_DS_PST DESC ) RANK,
        dau.REQUESTED_DS_PST AS CLOSEST_REQUEST,
        dau.DISBURSEMENT_DS_PST AS CLOSEST_DIS,
        CASE WHEN CLOSEST_DIS IS NULL THEN 1 ELSE 0 END AS MOST_RECENT_REQUEST_DECLINE,
        dau.PAYBACK_DS_PST AS CLOSEST_EXPECT_PAYBACK,
        dau.PREV_ADVANCE_BUCKET AS CLOSEST_PREV_ADVANCE_BUCKET,
        dau.MAX_APPROVED_AMOUNT,
        LAG(MAX_APPROVED_AMOUNT) OVER (PARTITION BY dau.USER_ID ORDER BY dau.REQUESTED_DS_PST DESC ) AS LAST_MAX_APPROVED_AMOUNT,
        COALESCE(dau.ADVANCE_TAKEN_AMOUNT, dau.OVERDRAFT_AMOUNT) AS ADVANCE_TAKEN_AMOUNT
    FROM ANALYTIC_DB.DBT_marts.dim_advance_users as dau
    JOIN USER_SUBSET_FORECAST USERS
    ON dau.USER_ID=USERS.USER_ID
    WHERE REQUESTED_DS_PST <USERS.FORECASTDATE
    QUALIFY RANK = 1
),
APPROVED_BANK_FORECAST AS (
    SELECT
        advance_requests.USER_ID,
        COUNT(DISTINCT
            IFF(TO_NUMERIC(advance_requests.MAX_APPROVED_AMOUNT) IS NOT NULL,
            advance_requests.BANK_ACCOUNT_ID, NULL)) AS APPROVED_BANK_COUNT
    FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
    ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
    ON advance_requests.advance_approval_id = o2_takeout.approval_id
    LEFT JOIN USER_SUBSET_FORECAST USERS
    ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
    WHERE ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
    GROUP BY 1
),
USER_TRANS_FORECAST AS
(
    SELECT
        USERS.USER_ID,
        USERS.FORECASTDATE,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
    FROM USER_SUBSET_FORECAST USERS
    JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE TO_DATE(TRANS_TIME) <USERS.FORECASTDATE
),
RFM_FORECAST AS
(
    SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), MAX(FORECASTDATE)) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY,
        ARRAY_TO_STRING(ARRAY_AGG(DATEDIFF('DAY', DATE(TRANS_TIME), FORECASTDATE)), ',') AS TRANS_LIST,
        ARRAY_TO_STRING(ARRAY_AGG(REVENUE), ',') AS MONETARY_LIST
    FROM USER_TRANS_FORECAST
    GROUP BY USER_ID
),
USERSESSIONMONTHLY_FORECAST AS
(
    SELECT
        USERS.USER_ID,
        DATE_TRUNC('month', ACTIVE.EVENT_DS_PST) AS MONTH,
        COUNT(ACTIVE.EVENT_DS_PST) AS MONTH_ACTIVE
    FROM USER_SUBSET_FORECAST USERS
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
    ON USERS.USER_ID = ACTIVE.USER_ID
    WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
    AND DATE(ACTIVE.EVENT_DS_PST) >= DATEADD('month', -6, USERS.FORECASTDATE)
    --GET THE SIX MONTHS SESSION NUMBER
    GROUP BY 1, 2
),
USERSESSION_FORECAST AS
(
    SELECT
        USER_ID,
        ARRAY_TO_STRING(ARRAY_AGG(MONTH_ACTIVE), ',') AS SESSION_LIST,
        SUM(MONTH_ACTIVE) AS SESSIONTOTAL
    FROM USERSESSIONMONTHLY_FORECAST
    GROUP BY USER_ID
),
FORECASTING AS
(
    SELECT
        USERS.USER_ID,
        USERS.STARTDATE,
        USERS.PLATFORM,
        USERS.ATTRIBUTION,
        USERS.NETWORK,
        USERS.BOD_ACCOUNT_OPEN_USER,
        USERS.BOD_DIRECT_DEPOSIT_USER,
        USERS.FORECASTDATE,
        BANKING.BANK_CATEGORY,
        BANKING.HAS_VALID_CREDENTIALS,
        UW.MOST_RECENT_REQUEST_DECLINE,
        UW.LAST_MAX_APPROVED_AMOUNT,
        UW.ADVANCE_TAKEN_AMOUNT,
        AB.APPROVED_BANK_COUNT,
        RFM.FIRST_TRANS,
        RFM.FREQUENCY,
        RFM.T,
        RFM.RECENCY,
        RFM.MONETARY,
        RFM.TRANS_LIST,
        RFM.MONETARY_LIST,
        SQ.SESSIONTOTAL,
        SQ.SESSION_LIST,
        0 AS REALREVENUE
    FROM USER_SUBSET_FORECAST USERS
    LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
    ON USERS.USER_ID = BANKING.USER_ID
    LEFT JOIN UW_FORECAST AS UW
    ON USERS.USER_ID = UW.USER_ID
    LEFT JOIN APPROVED_BANK_FORECAST AS AB
    ON USERS.USER_ID = AB.USER_ID
    LEFT JOIN RFM_FORECAST AS RFM
    ON USERS.USER_ID = RFM.USER_ID
    LEFT JOIN USERSESSION_FORECAST SQ
    ON USERS.USER_ID = SQ.USER_ID
)
SELECT * FROM TRAINING
UNION
SELECT * FROM FORECASTING


