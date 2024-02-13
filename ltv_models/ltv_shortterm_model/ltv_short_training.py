import os
import numpy as np
import pandas as pd
import davesci as ds
from sklearn.linear_model import LinearRegression, LogisticRegression
from ltv_short_udf import transform_x, feature_derived, transform_x_forecast
from ltv_short_global import log

SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE")

con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)
con_write = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)


def read_train_data():
    sql_str = """
    SELECT *
    FROM sandbox.DEV_YYANG_DBT_metrics.LTV_SHORT_INPUT
    WHERE FORECASTDATE < DATE_TRUNC('month', CURRENT_DATE())
    AND FIRST_TRANS IS NOT NULL
    AND (FREQUENCY = 0 OR (FREQUENCY > 0 AND RECENCY > 0))
    """
    df = pd.read_sql_query(sql_str, con)
    return df


def read_forecast_data():
    sql_str = """
    SELECT *
    FROM sandbox.DEV_YYANG_DBT_metrics.LTV_SHORT_INPUT
    WHERE FORECASTDATE = DATE_TRUNC('month', CURRENT_DATE())
    AND FIRST_TRANS IS NOT NULL
    AND (FREQUENCY = 0 OR (FREQUENCY > 0 AND RECENCY > 0))
    """
    df = pd.read_sql_query(sql_str, con)
    return df


def train_model(df):
    df = df.drop_duplicates(subset=["USER_ID"])
    test_date = np.sort(df["FORECASTDATE"].unique())
    df = feature_derived(df)
    dfupdate, scaler = transform_x(df)

    df_train = dfupdate.loc[dfupdate["FORECASTDATE"].isin(test_date[0:-1]), :]
    df_test = dfupdate.loc[dfupdate["FORECASTDATE"] == test_date[-1], :]
    x_columns = list(
        set(df_train.columns)
        - set(["USER_ID", "REVENUE", "LG_REVENUE", "FORECASTDATE"])
    )

    x_train = np.asarray(df_train.loc[:, x_columns].values).astype("float64")
    x_test = np.asarray(df_test.loc[:, x_columns].values).astype("float64")
    y_train = np.asarray(df_train["REVENUE"]).astype("float64")
    ly_train = np.asarray(df_train["LG_REVENUE"].astype(int))

    reg = LinearRegression()
    reg.fit(x_train, y_train)
    df_test["PREDICT"] = reg.predict(x_test)

    lreg = LogisticRegression(random_state=0).fit(x_train, ly_train)
    df_test[["LG_PREDICT_CHURN", "LG_PREDICT_RETENTION"]] = lreg.predict_proba(x_test)

    df_test_combine = pd.merge(
        df,
        df_test.loc[
            :, ["USER_ID", "PREDICT", "LG_PREDICT_CHURN", "LG_PREDICT_RETENTION"]
        ],
        on=["USER_ID"],
    )
    return reg, lreg, df_test_combine, x_columns, scaler


def forecast_model(df, reg, lreg, featurecolumn, scaler):
    df = df.drop_duplicates(subset=["USER_ID"])

    df = feature_derived(df)
    dfupdate, dfupdate_lr = transform_x_forecast(df, scaler)

    x_values = dfupdate.loc[:, featurecolumn].values
    x_values_lr = dfupdate_lr.loc[:, featurecolumn].values
    df["PREDICT"] = reg.predict(x_values)
    df[["LG_PREDICT_CHURN", "LG_PREDICT_RETENTION"]] = lreg.predict_proba(x_values_lr)

    return df


def main():

    dftrain = read_train_data()
    dfforecast = read_forecast_data()

    reg, lreg, dftest, x_columns, scaler = train_model(dftrain)
    dfforecastupdate = forecast_model(dfforecast, reg, lreg, x_columns, scaler)

    dfcombine = pd.concat([dftest, dfforecastupdate], axis=0, ignore_index=True)

    ds.write_snowflake_table(
        dfcombine,
        "SANDBOX.DEV_YYANG.ltvshort_forecast_result",
        con_write,
        mode="create",
    )

    log.info(dfforecastupdate.shape)


if __name__ == "__main__":
    main()
