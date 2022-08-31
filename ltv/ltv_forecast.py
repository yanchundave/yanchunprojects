import pandas as pd
import davesci as ds
from udf import ltv_statistical_model
from global_variable import *

SNOWFLAKE_ROLE = 'FUNC_ACCOUNTING_USER'
SNOWFLAKE_WAREHOUSE = 'DAVE_USER_WH'
con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)
con_write = ds.snowflake_connect(warehouse='DAVE_WH', role='DAVE_DATA_DEV')

def main():
    sql_str = """
    WITH
    USER AS
    (
        SELECT USER_ID FROM ACCOUNTING.DBT_LOCAL.LTV_USER_2022 WHERE startdate < date('2022-05-01')
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
        WHERE TRANS_TIME IS NOT NULL AND DATE(TRANS_TIME) < DATE('2022-08-25')
    )
    SELECT
    USER_ID,
    DATE(MIN(TRANS_TIME)) AS first_trans,
    COUNT(TRANS_ID) - 1 AS frequency,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('2022-08-25')) AS T,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS recency,
    AVG(REVENUE) AS monetary
    FROM USER_TRANS
    GROUP BY USER_ID;
    """

    user_sql = """
    SELECT
    USER_ID,
    STARTDATE,
    PLATFORM,
    ATTRIBUTION,
    NETWORK
    FROM ACCOUNTING.DBT_LOCAL.LTV_USER_2022
    """

    df = pd.read_sql_query(sql_str, con)
    df.columns = ['userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary']
    df_user = pd.read_sql_query(user_sql, con)
    df_user.columns = ['userid', 'startdate', 'platform', 'attribution', 'network']

    dftotal = ltv_statistical_model(df)
    dfconnect = pd.merge(df_user, dftotal, on=['userid'], how='left')
    dfconnect = dfconnect.fillna(0)

    dfconnect['start_date'] = pd.to_datetime(dfconnect['startdate'])
    dfconnect['start_month'] = dfconnect['start_date'].apply(lambda x: x.strftime('%Y-%m-01'))
    dfconnect['predict_label'] = dfconnect['first_trans'].apply(lambda x: 1 if x !=0 else 0)

    ds.write_snowflake_table(
        dfconnect,
        "ANALYTIC_DB.MODEL_OUTPUT.statistical_forecast_result",
        con_write,
        mode="create",
    )
    print("Done")

if __name__ == '__main__':
    main()