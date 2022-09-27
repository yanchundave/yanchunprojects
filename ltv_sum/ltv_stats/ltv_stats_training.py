import numpy as np
import pandas as pd
import davesci as ds
from ltv_stats_model import ltv_stats_model


SNOWFLAKE_ROLE = 'FUNC_ACCOUNTING_USER'
SNOWFLAKE_WAREHOUSE = 'DAVE_USER_WH'
con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)
con_write = ds.snowflake_connect(warehouse='DAVE_WH', role='DAVE_DATA_DEV')

def read_train_data():
    sql_str = """
    SELECT *
    FROM ACCOUNTING.DBT_LOCAL.LTV_STATS_INPUT
    WHERE FORECASTDATE = DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()))
    AND (FREQUENCY = 0 OR (FREQUENCY > 0 AND RECENCY > 0))
    """
    df = pd.read_sql_query(sql_str, con)
    return df

def read_forecast_data():
    sql_str = """
    SELECT *
    FROM ACCOUNTING.DBT_LOCAL.LTV_STATS_INPUT
    WHERE FORECASTDATE = DATE_TRUNC('month', CURRENT_DATE())
    AND (FREQUENCY = 0 OR (FREQUENCY > 0 AND RECENCY > 0))
    """
    df = pd.read_sql_query(sql_str, con)
    return df

def train_data(df):
    df_train = ltv_stats_model(df.loc[~df['FIRST_TRANS'].isnull(), ['USER_ID', 'FREQUENCY', 'T', 'RECENCY', 'MONETARY']])
    df_connect = pd.merge(df, df_train, on=['USER_ID'], how='left')
    return df_connect

def main():
    training_data = read_train_data()
    df_train_result = train_data(training_data)

    forecast_data = read_forecast_data()
    df_forecast_result = train_data(forecast_data)

    df_total = pd.concat([df_train_result, df_forecast_result], axis=0)

    ds.write_snowflake_table(
        df_total,
        "ANALYTIC_DB.MODEL_OUTPUT.statistical_forecast_result",
        con_write,
        mode="create",
    )
    print("Done")

if __name__ == '__main__':
    main()