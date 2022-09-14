import pandas as pd
import davesci as ds
from udf import ltv_statistical_model
from global_variable import *
import random
import numpy as np

SNOWFLAKE_ROLE = 'FUNC_ACCOUNTING_USER'
SNOWFLAKE_WAREHOUSE = 'DAVE_USER_WH'
con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)
con_write = ds.snowflake_connect(warehouse='DAVE_WH', role='DAVE_DATA_DEV')
FORECASTDATE = '2022-09-01'
USER_CORHORT = '2022-06-01'

def read_data():
    sql_str = f"""
    WITH
    USER AS
    (
        SELECT USER_ID FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022 WHERE startdate < date('{USER_CORHORT}')
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
        WHERE TRANS_TIME IS NOT NULL AND DATE(TRANS_TIME) < DATE('{FORECASTDATE}')
    )
    SELECT
    USER_ID,
    DATE(MIN(TRANS_TIME)) AS first_trans,
    COUNT(TRANS_ID) - 1 AS frequency,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('{FORECASTDATE}')) AS T,
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS recency,
    AVG(REVENUE) AS monetary,
    SUM(REVENUE) AS totalrevenue
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
    FROM DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022
    """

    df = pd.read_sql_query(sql_str, con)
    df.columns = ['userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary', 'totalrevenue']
    df_user = pd.read_sql_query(user_sql, con)
    df_user.columns = ['userid', 'startdate', 'platform', 'attribution', 'network']

    return df, df_user

def train_data():
    df = pd.read_csv(datafile + "forecast_data.csv")
    df_user = pd.read_csv(datafile + "forecast_user_data.csv")
    dftotal = ltv_statistical_model(df.loc[:, ['userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary']])
    df_revenue = df.loc[:, ['userid', 'totalrevenue']]
    dfconnect_pre = pd.merge(df_user, dftotal, on=['userid'], how='left')
    dfconnect = pd.merge(dfconnect_pre, df_revenue, on=['userid'], how='left')
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

def sample_data():
    df = pd.read_csv(datafile + "forecast_data.csv")
    df_user = pd.read_csv(datafile + "forecast_user_data.csv")
    arpus = []
    churns = []
    for i in range(0, 100):
        df_pos = df[(df['T'] > 200)]
        print(i)
        df_ng = df[(df['T'] <= 200)].sample(frac=random.uniform(0, 1), random_state=i)
        dfupdate = pd.concat([df_pos, df_ng], axis=0)
        dftotal = ltv_statistical_model(dfupdate.loc[:, ['userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary']])
        df_revenue = df.loc[:, ['userid', 'totalrevenue']]
        if dftotal is not None:
            dfconnect = pd.merge(dftotal, df_revenue, on=['userid'])
            dfconnect = dfconnect.fillna(0)
            dfconnect['churn_predict'] = dfconnect['pred_num'].apply(lambda x: 1 if x < 2 else 0)
            arpus.append(np.mean(dfconnect['t_value']))
            churns.append(np.mean(dfconnect['churn_predict']))
    result = pd.DataFrame.from_dict({"arpu": arpus, "churn": churns})
    result.to_csv(datafile + "forecast_sample.csv")


def main():
    #df, df_user = read_data()
    #df.to_csv(datafile + "forecast_data.csv")
    #df_user.to_csv(datafile + "forecast_user_data.csv")
    #train_data()
    sample_data()

if __name__ == '__main__':
    main()