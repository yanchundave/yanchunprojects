from statistics import LinearRegression
import numpy as np
import pandas as pd
import davesci as ds
from ltv_long_udf import transform_x, split_train_test, feature_derived, transform_x_forecast
from sklearn.linear_model import LinearRegression, LogisticRegression
import pickle
from ltv_long_global import *

# NEED REMOVE NO ADVANCE USER
SNOWFLAKE_ROLE = 'FUNC_ACCOUNTING_USER'
SNOWFLAKE_WAREHOUSE = 'DAVE_USER_WH'
con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)
con_write = ds.snowflake_connect(warehouse='DAVE_WH', role='DAVE_DATA_DEV')

def read_train_data():
    sql_str = """
    SELECT *
    FROM ACCOUNTING.DBT_LOCAL.LTV_LONG_INPUT
    WHERE FORECASTDATE = DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()))
    AND FIRST_TRANS IS NOT NULL
    """
    df = pd.read_sql_query(sql_str, con)
    return df

def read_forecast_data():
    sql_str = """
    SELECT *
    FROM ACCOUNTING.DBT_LOCAL.LTV_LONG_INPUT
    WHERE FORECASTDATE = DATE_TRUNC('month', CURRENT_DATE())
    AND FIRST_TRANS IS NOT NULL
    """
    df = pd.read_sql_query(sql_str, con)
    return df


def train_model(df):
    df = df.drop_duplicates(subset=['USER_ID'])

    df = feature_derived(df)
    dfupdate, scaler = transform_x(df)
    df_train, df_test = split_train_test(dfupdate, 0.2)

    x_columns = list(set(df_train.columns) - set(['USER_ID', 'REALREVENUE', 'LG_REVENUE']))

    x_train = df_train.loc[:, x_columns].values
    x_test = df_test.loc[:, x_columns].values
    y_train = df_train['REALREVENUE']
    ly_train = df_train['LG_REVENUE']

    reg = LinearRegression()
    reg.fit(x_train, y_train)
    df_test['PREDICT'] = reg.predict(x_test)

    lreg = LogisticRegression(random_state=0).fit(x_train, ly_train)
    df_test[['LG_PREDICT_CHURN', 'LG_PREDICT_RETENTION']] = lreg.predict_proba(x_test)

    df_test_combine = pd.merge(df, df_test.loc[:, ['USER_ID', 'PREDICT', 'LG_PREDICT_CHURN', 'LG_PREDICT_RETENTION']], on=['USER_ID'])
    return reg, lreg, df_test_combine, x_columns, scaler

def forecast_model(df, reg, lreg, featurecolumn, scaler):
    df = df.drop_duplicates(subset=['USER_ID'])

    df = feature_derived(df)
    dfupdate, dfupdate_lr = transform_x_forecast(df, scaler)

    x_values = dfupdate.loc[:, featurecolumn].values
    x_values_lr = dfupdate_lr.loc[:, featurecolumn].values
    df['PREDICT'] = reg.predict(x_values)
    df[['LG_PREDICT_CHURN', 'LG_PREDICT_RETENTION']] = lreg.predict_proba(x_values_lr)

    return df

def main():
    """
    dftrain = read_train_data()
    dfforecast = read_forecast_data()

    with open(datafile_path + "dftrain.pk", "wb") as f:
        pickle.dump(dftrain, f)
    with open(datafile_path + "dfforecast.pk", "wb") as f:
        pickle.dump(dfforecast, f)
    """

    with open(datafile_path + "dftrain.pk", "rb") as f:
        dftrain = pickle.load(f)
    with open(datafile_path + "dfforecast.pk", "rb") as f:
        dfforecast = pickle.load(f)


    reg, lreg, dftest, x_columns, scaler = train_model(dftrain)
    dfforecastupdate = forecast_model(dfforecast, reg, lreg, x_columns, scaler)

    dfcombine = pd.concat([dftest, dfforecastupdate], axis=0, ignore_index=True)

    ds.write_snowflake_table(
        dfcombine,
        "ANALYTIC_DB.MODEL_OUTPUT.ltvlong_forecast_result",
        con_write,
        mode="create",
    )
    print("Done")

if __name__ == '__main__':
    main()