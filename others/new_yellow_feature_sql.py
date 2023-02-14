import sys

START_DIFF = 3
DISTANCE = 6
END_DIFF = START_DIFF + DISTANCE

def generate_training_data():
    forecast_datasets = ['2021-04-01', '2021-07-01', '2021-10-01', '2021-12-01']
    with open("yellow_feature_sql.txt", 'a') as f:
        for item in forecast_datasets:
            sql = f"""
            DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.LTV_USER_NEWQUARTER_{item[5:7]};
            CREATE TABLE ACCOUNTING.DBT_LOCAL.LTV_USER_NEWQUARTER_{item[5:7]}
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
            DATE('{item}') AS FORECASTDATE,
            DATEADD('month', -3, DATE('{item}')) AS LOWERDATE,
            DATEADD('month', {START_DIFF}, DATE('{item}')) AS TRANS_STARTDATE,
            DATEADD('month', {END_DIFF}, DATE('{item}')) AS ENDDATE
            FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
            WHERE STARTDATE < DATE('{item}') AND STARTDATE >= LOWERDATE
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
            lag(max_approved_amount)over (partition by dau.user_id order by dau.requested_ds_pst DESC ) AS LAST_MAX_APPROVED_AMOUNT,
            COALESCE(dau.advance_taken_amount, dau.overdraft_amount) AS ADVANCE_TAKEN_AMOUNT
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
                advance_requests.bank_account_id, NULL)) AS approved_bank_count
            FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
            LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
            ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
            LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
            ON advance_requests.advance_approval_id = o2_takeout.approval_id
            LEFT JOIN USER_SUBSET USERS
            ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
            where ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
            group by 1
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
            WHERE to_date(TRANS_TIME) <USERS.FORECASTDATE
            ),
            RFM AS (
            SELECT
            USER_ID,
            DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
            COUNT(TRANS_ID) - 1 AS FREQUENCY,
            DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('{item}')) AS T,
            DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
            AVG(REVENUE) AS MONETARY
            FROM USER_TRANS
            GROUP BY USER_ID
            ),
            USERSESSION AS
            (
            SELECT USERS.USER_ID, COUNT(ACTIVE.EVENT_DS_PST) AS ACTIVESESSION
            FROM USER_SUBSET USERS
            JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
            ON USERS.USER_ID = ACTIVE.USER_ID
            WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
            GROUP BY USERS.USER_ID
            ),
            UR AS (
            SELECT USERS.USER_ID, SUM(REVENUE) AS REVENUE
            FROM USER_SUBSET USERS
            LEFT JOIN ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022 TRANS
            ON USERS.USER_ID = TRANS.USER_ID
            WHERE DATE(TRANS.TRANS_TIME) >= DATE(USERS.TRANS_STARTDATE) AND DATE(TRANS.TRANS_TIME) < DATE(USERS.ENDDATE)
            GROUP BY USERS.USER_ID
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
            USERS.LOWERDATE,
            USERS.TRANS_STARTDATE,
            USERS.ENDDATE,
            BANKING.BANK_CATEGORY,
            BANKING.HAS_VALID_CREDENTIALS,
            UW.MOST_RECENT_REQUEST_DECLINE,
            UW.LAST_MAX_APPROVED_AMOUNT,
            UW.ADVANCE_TAKEN_AMOUNT,
            RFM.FIRST_TRANS,
            RFM.FREQUENCY,
            RFM.T,
            RFM.RECENCY,
            RFM.MONETARY,
            SQ.ACTIVESESSION,
            UR.REVENUE
            FROM USER_SUBSET USERS
            LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
            ON USERS.USER_ID = BANKING.USER_ID
            LEFT JOIN UW
            ON USERS.USER_ID = UW.USER_ID
            LEFT JOIN APPROVED_BANK
            ON USERS.USER_ID = APPROVED_BANK.USER_ID
            LEFT JOIN RFM
            ON USERS.USER_ID = RFM.USER_ID
            LEFT JOIN USERSESSION SQ
            ON USERS.USER_ID = SQ.USER_ID
            LEFT JOIN UR
            ON USERS.USER_ID = UR.USER_ID
            """
            f.write(sql)
            f.write("----------------\n")

def generate_newdata():
    FORECASTDATE = '2022-09-01'
    with open("yellow_feature_sql.txt", 'a') as f:
        sql = f"""
        DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.LTV_USER_NEWQUARTER_CANDIDATE;
        CREATE TABLE ACCOUNTING.DBT_LOCAL.LTV_USER_NEWQUARTER_CANDIDATE
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
        DATE('{FORECASTDATE}') AS FORECASTDATE,
        DATEADD('month', -3, DATE('{FORECASTDATE}')) AS LOWERDATE
        FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 USER
        WHERE STARTDATE < DATE('{FORECASTDATE}') AND STARTDATE >= LOWERDATE
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
        lag(max_approved_amount)over (partition by dau.user_id order by dau.requested_ds_pst DESC ) AS LAST_MAX_APPROVED_AMOUNT,
        COALESCE(dau.advance_taken_amount, dau.overdraft_amount) AS ADVANCE_TAKEN_AMOUNT
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
            advance_requests.bank_account_id, NULL)) AS approved_bank_count
        FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
        LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
        ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
        LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
        ON advance_requests.advance_approval_id = o2_takeout.approval_id
        LEFT JOIN USER_SUBSET USERS
        ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
        where ADVANCE_REQUESTS.REQUESTED_DS<USERS.FORECASTDATE
        group by 1
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
        WHERE to_date(TRANS_TIME) <USERS.FORECASTDATE
        ),
        RFM AS (
        SELECT
        USER_ID,
        DATE(MIN(TRANS_TIME)) AS FIRST_TRANS,
        COUNT(TRANS_ID) - 1 AS FREQUENCY,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('{FORECASTDATE}')) AS T,
        DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS RECENCY,
        AVG(REVENUE) AS MONETARY
        FROM USER_TRANS
        GROUP BY USER_ID
        ),
        USERSESSION AS
        (
        SELECT USERS.USER_ID, COUNT(ACTIVE.EVENT_DS_PST) AS ACTIVESESSION
        FROM USER_SUBSET USERS
        JOIN ANALYTIC_DB.DBT_METRICS.ONE_DAVE_ACTIVE_USERS ACTIVE
        ON USERS.USER_ID = ACTIVE.USER_ID
        WHERE DATE(ACTIVE.EVENT_DS_PST) < USERS.FORECASTDATE
        GROUP BY USERS.USER_ID
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
        USERS.LOWERDATE,
        BANKING.BANK_CATEGORY,
        BANKING.HAS_VALID_CREDENTIALS,
        UW.MOST_RECENT_REQUEST_DECLINE,
        UW.LAST_MAX_APPROVED_AMOUNT,
        UW.ADVANCE_TAKEN_AMOUNT,
        RFM.FIRST_TRANS,
        RFM.FREQUENCY,
        RFM.T,
        RFM.RECENCY,
        RFM.MONETARY,
        SQ.ACTIVESESSION,
        0 AS REVENUE
        FROM USER_SUBSET USERS
        LEFT JOIN DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION BANKING
        ON USERS.USER_ID = BANKING.USER_ID
        LEFT JOIN UW
        ON USERS.USER_ID = UW.USER_ID
        LEFT JOIN APPROVED_BANK
        ON USERS.USER_ID = APPROVED_BANK.USER_ID
        LEFT JOIN RFM
        ON USERS.USER_ID = RFM.USER_ID
        LEFT JOIN USERSESSION SQ
        ON USERS.USER_ID = SQ.USER_ID
        """
        f.write(sql)
        f.write("----------------\n")

generate_training_data()

generate_newdata()
