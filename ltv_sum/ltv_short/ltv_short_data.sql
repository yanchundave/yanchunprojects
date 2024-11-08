DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.LTV_SHORT_INPUT;
CREATE TABLE ACCOUNTING.DBT_LOCAL.LTV_SHORT_INPUT
AS
WITH USER_SUBSET_TEST AS
(
    SELECT
        USER_ID,
        STARTDATE,
        PLATFORM,
        ATTRIBUTION,
        NETWORK,
        BOD_ACCOUNT_OPEN_USER,
        BOD_DIRECT_DEPOSIT_USER,
        DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE())) AS FORECASTDATE,
        DATEADD('month', -3, FORECASTDATE) AS USERSTARTDATE,
        DATEADD('month', 6, FORECASTDATE) AS TRANSENDDATE
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
    WHERE STARTDATE < FORECASTDATE AND STARTDATE >= USERSTARTDATE
),
UW_TEST AS
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
    JOIN USER_SUBSET_TEST USERS
    ON dau.USER_ID=USERS.USER_ID
    WHERE REQUESTED_DS_PST <USERS.FORECASTDATE
    QUALIFY RANK = 1
),
APPROVED_BANK_TEST AS (
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
    LEFT JOIN USER_SUBSET_TEST USERS
    ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
    WHERE ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
    GROUP BY 1
),
USER_TRANS_TEST AS
(
    SELECT
        USERS.USER_ID,
        USERS.FORECASTDATE,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
    FROM USER_SUBSET_TEST USERS
    JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE to_date(TRANS_TIME) <USERS.FORECASTDATE
),
RFM_TEST AS (
    SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), MAX(FORECASTDATE)) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY
    FROM USER_TRANS_TEST
    GROUP BY USER_ID
),
USERSESSION_TEST AS
(
    SELECT
        USERS.USER_ID,
        COUNT(ACTIVE.EVENT_DS_PST) AS ACTIVESESSION
    FROM USER_SUBSET_TEST USERS
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
    ON USERS.USER_ID = ACTIVE.USER_ID
    WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
    GROUP BY USERS.USER_ID
),
UR_TEST AS
(
    SELECT
        USERS.USER_ID,
        SUM(REVENUE) AS REVENUE
    FROM USER_SUBSET_TEST USERS
    LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE DATE(TRANS.TRANS_TIME) >= DATE(USERS.FORECASTDATE) AND DATE(TRANS.TRANS_TIME) < DATE(USERS.TRANSENDDATE)
    GROUP BY USERS.USER_ID
),
TEST_DATA AS
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
        USERS.USERSTARTDATE,
        USERS.TRANSENDDATE,
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
        SQ.ACTIVESESSION,
        UR.REVENUE
    FROM USER_SUBSET_TEST USERS
    LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
    ON USERS.USER_ID = BANKING.USER_ID
    LEFT JOIN UW_TEST AS UW
    ON USERS.USER_ID = UW.USER_ID
    LEFT JOIN APPROVED_BANK_TEST AS AB
    ON USERS.USER_ID = AB.USER_ID
    LEFT JOIN RFM_TEST AS RFM
    ON USERS.USER_ID = RFM.USER_ID
    LEFT JOIN USERSESSION_TEST SQ
    ON USERS.USER_ID = SQ.USER_ID
    LEFT JOIN UR_TEST AS UR
    ON USERS.USER_ID = UR.USER_ID
),
----TRAIN 1
USER_SUBSET_TRAINA AS
(
    SELECT
        USER_ID,
        STARTDATE,
        PLATFORM,
        ATTRIBUTION,
        NETWORK,
        BOD_ACCOUNT_OPEN_USER,
        BOD_DIRECT_DEPOSIT_USER,
        DATEADD('month', -9, DATE_TRUNC('month', CURRENT_DATE())) AS FORECASTDATE,
        DATEADD('month', -3, FORECASTDATE) AS USERSTARTDATE,
        DATEADD('month', 6, FORECASTDATE) AS TRANSENDDATE
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
    WHERE STARTDATE < FORECASTDATE AND STARTDATE >= USERSTARTDATE
),
UW_TRAINA AS
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
    JOIN USER_SUBSET_TRAINA USERS
    ON dau.USER_ID=USERS.USER_ID
    WHERE REQUESTED_DS_PST <USERS.FORECASTDATE
    QUALIFY RANK = 1
),
APPROVED_BANK_TRAINA AS (
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
    LEFT JOIN USER_SUBSET_TRAINA USERS
    ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
    WHERE ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
    GROUP BY 1
),
USER_TRANS_TRAINA AS
(
    SELECT
        USERS.USER_ID,
        USERS.FORECASTDATE,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
    FROM USER_SUBSET_TRAINA USERS
    JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE to_date(TRANS_TIME) <USERS.FORECASTDATE
),
RFM_TRAINA AS (
    SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), MAX(FORECASTDATE)) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY
    FROM USER_TRANS_TRAINA
    GROUP BY USER_ID
),
USERSESSION_TRAINA AS
(
    SELECT
        USERS.USER_ID,
        COUNT(ACTIVE.EVENT_DS_PST) AS ACTIVESESSION
    FROM USER_SUBSET_TRAINA USERS
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
    ON USERS.USER_ID = ACTIVE.USER_ID
    WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
    GROUP BY USERS.USER_ID
),
UR_TRAINA AS
(
    SELECT
        USERS.USER_ID,
        SUM(REVENUE) AS REVENUE
    FROM USER_SUBSET_TRAINA USERS
    LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE DATE(TRANS.TRANS_TIME) >= DATE(USERS.FORECASTDATE) AND DATE(TRANS.TRANS_TIME) < DATE(USERS.TRANSENDDATE)
    GROUP BY USERS.USER_ID
),
TRAINA_DATA AS
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
        USERS.USERSTARTDATE,
        USERS.TRANSENDDATE,
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
        SQ.ACTIVESESSION,
        UR.REVENUE
    FROM USER_SUBSET_TRAINA USERS
    LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
    ON USERS.USER_ID = BANKING.USER_ID
    LEFT JOIN UW_TRAINA AS UW
    ON USERS.USER_ID = UW.USER_ID
    LEFT JOIN APPROVED_BANK_TRAINA AS AB
    ON USERS.USER_ID = AB.USER_ID
    LEFT JOIN RFM_TRAINA AS RFM
    ON USERS.USER_ID = RFM.USER_ID
    LEFT JOIN USERSESSION_TRAINA SQ
    ON USERS.USER_ID = SQ.USER_ID
    LEFT JOIN UR_TRAINA AS UR
    ON USERS.USER_ID = UR.USER_ID
),
--TRAIN_B
USER_SUBSET_TRAINB AS
(
    SELECT
        USER_ID,
        STARTDATE,
        PLATFORM,
        ATTRIBUTION,
        NETWORK,
        BOD_ACCOUNT_OPEN_USER,
        BOD_DIRECT_DEPOSIT_USER,
        DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())) AS FORECASTDATE,
        DATEADD('month', -3, FORECASTDATE) AS USERSTARTDATE,
        DATEADD('month', 6, FORECASTDATE) AS TRANSENDDATE
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
    WHERE STARTDATE < FORECASTDATE AND STARTDATE >= USERSTARTDATE
),
UW_TRAINB AS
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
    JOIN USER_SUBSET_TRAINB USERS
    ON dau.USER_ID=USERS.USER_ID
    WHERE REQUESTED_DS_PST <USERS.FORECASTDATE
    QUALIFY RANK = 1
),
APPROVED_BANK_TRAINB AS (
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
    LEFT JOIN USER_SUBSET_TRAINB USERS
    ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
    WHERE ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
    GROUP BY 1
),
USER_TRANS_TRAINB AS
(
    SELECT
        USERS.USER_ID,
        USERS.FORECASTDATE,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
    FROM USER_SUBSET_TRAINB USERS
    JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE to_date(TRANS_TIME) <USERS.FORECASTDATE
),
RFM_TRAINB AS (
    SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), MAX(FORECASTDATE)) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY
    FROM USER_TRANS_TRAINB
    GROUP BY USER_ID
),
USERSESSION_TRAINB AS
(
    SELECT
        USERS.USER_ID,
        COUNT(ACTIVE.EVENT_DS_PST) AS ACTIVESESSION
    FROM USER_SUBSET_TRAINB USERS
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
    ON USERS.USER_ID = ACTIVE.USER_ID
    WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
    GROUP BY USERS.USER_ID
),
UR_TRAINB AS
(
    SELECT
        USERS.USER_ID,
        SUM(REVENUE) AS REVENUE
    FROM USER_SUBSET_TRAINB USERS
    LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE DATE(TRANS.TRANS_TIME) >= DATE(USERS.FORECASTDATE) AND DATE(TRANS.TRANS_TIME) < DATE(USERS.TRANSENDDATE)
    GROUP BY USERS.USER_ID
),
TRAINB_DATA AS
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
        USERS.USERSTARTDATE,
        USERS.TRANSENDDATE,
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
        SQ.ACTIVESESSION,
        UR.REVENUE
    FROM USER_SUBSET_TRAINB USERS
    LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
    ON USERS.USER_ID = BANKING.USER_ID
    LEFT JOIN UW_TRAINB AS UW
    ON USERS.USER_ID = UW.USER_ID
    LEFT JOIN APPROVED_BANK_TRAINB AS AB
    ON USERS.USER_ID = AB.USER_ID
    LEFT JOIN RFM_TRAINB AS RFM
    ON USERS.USER_ID = RFM.USER_ID
    LEFT JOIN USERSESSION_TRAINB SQ
    ON USERS.USER_ID = SQ.USER_ID
    LEFT JOIN UR_TRAINB AS UR
    ON USERS.USER_ID = UR.USER_ID
),
--TRAIN_C
USER_SUBSET_TRAINC AS
(
    SELECT
        USER_ID,
        STARTDATE,
        PLATFORM,
        ATTRIBUTION,
        NETWORK,
        BOD_ACCOUNT_OPEN_USER,
        BOD_DIRECT_DEPOSIT_USER,
        DATEADD('month', -15, DATE_TRUNC('month', CURRENT_DATE())) AS FORECASTDATE,
        DATEADD('month', -3, FORECASTDATE) AS USERSTARTDATE,
        DATEADD('month', 6, FORECASTDATE) AS TRANSENDDATE
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
    WHERE STARTDATE < FORECASTDATE AND STARTDATE >= USERSTARTDATE
),
UW_TRAINC AS
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
    JOIN USER_SUBSET_TRAINC USERS
    ON dau.USER_ID=USERS.USER_ID
    WHERE REQUESTED_DS_PST <USERS.FORECASTDATE
    QUALIFY RANK = 1
),
APPROVED_BANK_TRAINC AS (
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
    LEFT JOIN USER_SUBSET_TRAINC USERS
    ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
    WHERE ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
    GROUP BY 1
),
USER_TRANS_TRAINC AS
(
    SELECT
        USERS.USER_ID,
        USERS.FORECASTDATE,
        TRANS_TIME,
        TRANS_ID,
        REVENUE
    FROM USER_SUBSET_TRAINC USERS
    JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE to_date(TRANS_TIME) <USERS.FORECASTDATE
),
RFM_TRAINC AS (
    SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), MAX(FORECASTDATE)) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY
    FROM USER_TRANS_TRAINC
    GROUP BY USER_ID
),
USERSESSION_TRAINC AS
(
    SELECT
        USERS.USER_ID,
        COUNT(ACTIVE.EVENT_DS_PST) AS ACTIVESESSION
    FROM USER_SUBSET_TRAINC USERS
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
    ON USERS.USER_ID = ACTIVE.USER_ID
    WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
    GROUP BY USERS.USER_ID
),
UR_TRAINC AS
(
    SELECT
        USERS.USER_ID,
        SUM(REVENUE) AS REVENUE
    FROM USER_SUBSET_TRAINC USERS
    LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
    ON USERS.USER_ID = TRANS.USER_ID
    WHERE DATE(TRANS.TRANS_TIME) >= DATE(USERS.FORECASTDATE) AND DATE(TRANS.TRANS_TIME) < DATE(USERS.TRANSENDDATE)
    GROUP BY USERS.USER_ID
),
TRAINC_DATA AS
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
        USERS.USERSTARTDATE,
        USERS.TRANSENDDATE,
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
        SQ.ACTIVESESSION,
        UR.REVENUE
    FROM USER_SUBSET_TRAINC USERS
    LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
    ON USERS.USER_ID = BANKING.USER_ID
    LEFT JOIN UW_TRAINC AS UW
    ON USERS.USER_ID = UW.USER_ID
    LEFT JOIN APPROVED_BANK_TRAINC AS AB
    ON USERS.USER_ID = AB.USER_ID
    LEFT JOIN RFM_TRAINC AS RFM
    ON USERS.USER_ID = RFM.USER_ID
    LEFT JOIN USERSESSION_TRAINC SQ
    ON USERS.USER_ID = SQ.USER_ID
    LEFT JOIN UR_TRAINC AS UR
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
        DATE_TRUNC('month', CURRENT_DATE()) AS FORECASTDATE,
        DATEADD('month', -3, FORECASTDATE) AS USERSTARTDATE,
        DATEADD('month', 6, FORECASTDATE) AS TRANSENDDATE
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
    WHERE STARTDATE < FORECASTDATE AND STARTDATE >= USERSTARTDATE
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
    WHERE to_date(TRANS_TIME) <USERS.FORECASTDATE
),
RFM_FORECAST AS (
    SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), MAX(FORECASTDATE)) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY
    FROM USER_TRANS_FORECAST
    GROUP BY USER_ID
),
USERSESSION_FORECAST AS
(
    SELECT
        USERS.USER_ID,
        COUNT(ACTIVE.EVENT_DS_PST) AS ACTIVESESSION
    FROM USER_SUBSET_FORECAST USERS
    JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
    ON USERS.USER_ID = ACTIVE.USER_ID
    WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
    GROUP BY USERS.USER_ID
),
FORECAST_DATA AS
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
        USERS.USERSTARTDATE,
        USERS.TRANSENDDATE,
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
        SQ.ACTIVESESSION,
        0 AS REVENUE
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
SELECT * FROM TEST_DATA
UNION
SELECT * FROM TRAINA_DATA
UNION
SELECT * FROM TRAINB_DATA
UNION
SELECT * FROM TRAINC_DATA
UNION
SELECT * FROM FORECAST_DATA