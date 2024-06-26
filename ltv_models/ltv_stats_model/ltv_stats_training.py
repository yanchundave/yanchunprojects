import os

import pandas as pd
import davesci as ds
from ltv_stats_model import ltv_stats_model
from ltv_stats_global import log

SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE")

con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)
con_write = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)


def read_train_data():
    sql_str = """
    SELECT *
    FROM sandbox.DEV_YYANG_DBT_metrics.LTV_STATS_INPUT
    WHERE FORECASTDATE = DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()))
    AND FIRST_TRANS IS NOT NULL
    AND (FREQUENCY = 0 OR (FREQUENCY > 0 AND RECENCY > 0))
    """
    df = pd.read_sql_query(sql_str, con)
    return df


def read_forecast_data():
    sql_str = """
    SELECT *
    FROM sandbox.DEV_YYANG_DBT_metrics.LTV_STATS_INPUT
    WHERE FORECASTDATE = DATE_TRUNC('month', CURRENT_DATE())
    AND FIRST_TRANS IS NOT NULL
    AND (FREQUENCY = 0 OR (FREQUENCY > 0 AND RECENCY > 0))
    """
    df = pd.read_sql_query(sql_str, con)
    return df


def train_data(df):
    df_train = ltv_stats_model(
        df.loc[
            ~df["FIRST_TRANS"].isnull(),
            ["USER_ID", "FREQUENCY", "T", "RECENCY", "MONETARY"],
        ]
    )
    if df_train is not None:
        df_connect = pd.merge(df, df_train, on=["USER_ID"], how="left")
    else:
        df_connect = df
    return df_connect


def main():
    training_data = read_train_data()
    df_train_result = train_data(training_data)

    forecast_data = read_forecast_data()
    df_forecast_result = train_data(forecast_data)

    df_total = pd.concat([df_train_result, df_forecast_result], axis=0)

    ds.write_snowflake_table(
        df_total,
        "SANDBOX.DEV_YYANG.statistical_forecast_result",
        con_write,
        mode="create",
    )
    log.info(df_forecast_result.shape)


if __name__ == "__main__":
    main()
