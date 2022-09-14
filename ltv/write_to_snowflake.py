import pandas as pd
import davesci as ds


con_write = ds.snowflake_connect(warehouse='DAVE_WH', role='DAVE_DATA_DEV')
END_DATE = '2022-09-01'
START_DATE = '2022-06-01'
datafile_path = "/Users/yanchunyang/Documents/datafiles/ltv_ml/"
con_write = ds.snowflake_connect(warehouse='DAVE_WH', role='DAVE_DATA_DEV')

def main():
    df = pd.read_csv(datafile_path + "regression_newuser_predict.csv", header=0)

    ds.write_snowflake_table(
        df,
        "ANALYTIC_DB.MODEL_OUTPUT.ml_forecast_result",
        con_write,
        mode="create",
    )
    print("Done")

if __name__ == '__main__':
    main()