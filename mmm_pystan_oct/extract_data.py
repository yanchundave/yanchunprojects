import jaydebeapi as jay
import numpy as np
import pandas as pd
from global_variable import *
from datetime import datetime, timedelta
from mmm_udf import pivot_table

"""
Step 1: to extract input table that MMM needed
"""

def read_from_snowflake(sql_str):
    with open('/Users/yanchunyang/pwd/snowflake.passphrase', 'r') as f:
        passphrase = f.read().strip()
    username = "yanchun.yang@dave.com"
    password = "abc"
    jdbcpath = "/Users/yanchunyang/lib/jdbc/snowflake-jdbc-3.13.8.jar"
    jdbc_driver_name = "net.snowflake.client.jdbc.SnowflakeDriver"
    hostname= "qc63563.snowflakecomputing.com"
    role = "DAVE_DATA_DEV"
    warehouse = "DAVE_ANALYTICS_WH"
    keyfile = "/Users/yanchunyang/.ssh/snowflake.p8"

    conn_string = f'jdbc:snowflake://qc63563.snowflakecomputing.com?role={role}&warehouse={warehouse}&private_key_file={keyfile}&private_key_file_pwd={passphrase}'

    conn = jay.connect(jdbc_driver_name, conn_string, {'user': username , 'password': password }, jars=jdbcpath)

#  Currently python can't interpret correctly the result returned from JDBC to connect Snowflake so we have to switch back to JSON rather than ARROW format
# It can be done at session level
    session_set = "ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'"
    curs = conn.cursor()
    curs.execute(session_set)

    curs.execute(sql_str)
    result = curs.fetchall()
    return result


def extract_spending_data(origin_date, end_date):
    """
        Extract channel and platform spending data. Independent variables in the model.
    """

    sql_str = f"""
    SELECT
        DATE(SPEND_DATE_PACIFIC_TIME) AS SPENDDATE,
        NETWORK,
        PLATFORM,
        SUM(SPEND) AS TOTALSPEND
    FROM ANALYTIC_DB.DBT_MARTS.MARKETING_SPEND
    WHERE SPEND_DATE_PACIFIC_TIME>= DATE('{origin_date}')
    AND SPEND_DATE_PACIFIC_TIME <= DATE('{end_date}')
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3;
    """

    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['date', 'channel', 'platform', 'spending'])
    values = {'platform': 'unknown', 'spending':0}
    df = df.fillna(value=values)
    #df.to_csv(datafile_path + "platform_raw.csv")
    return df


def extract_user_data(origin_date, end_date, user_type):
    """
        Extract advance users or one-month revenue after PV. Dependent variables in the model.
    """
    # One dave query which has be degenerated. It was used in MMM version 1.
    sql_str_onedave = f"""
    SELECT
        LEFT(CONVERT_TIMEZONE('UTC','America/Los_Angeles',PV_TS::timestamp_ntz) , 10) AS date,
        SUM(CASE WHEN PV_TS IS NOT NULL THEN 1 ELSE 0 END) AS PV
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE TO_DATE(CONVERT_TIMEZONE('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='{origin_date}'
          AND TO_DATE(CONVERT_TIMEZONE('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))<='{end_date}'
    GROUP BY 1
    ORDER BY 1;
    """

    # advance user query
    sql_str_advance =f"""
    SELECT
        LEFT(CONVERT_TIMEZONE('UTC','America/Los_Angeles',PV_TS::timestamp_ntz) , 10) AS date,
        SUM(CASE WHEN ADVANCE_TAKEN_USER IS NOT NULL THEN 1 ELSE 0 END) AS PV
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE TO_DATE(CONVERT_TIMEZONE('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='{origin_date}'
          AND TO_DATE(CONVERT_TIMEZONE('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))<='{end_date}'
    GROUP BY 1
    ORDER BY 1;
    """
    sql_str = sql_str_advance if user_type == 'advance' else sql_str_onedave
    results = read_from_snowflake(sql_str)

    df = pd.DataFrame(results, columns=['date', 'PV'])
    df = df.fillna(0)
    df.to_csv(datafile_path + "platform_user_advance.csv")
    print("Done")


def extract_revenue_data(origin_date, end_date):
    sql_str = f"""
    WITH user_pv AS
    (
        SELECT
            USER_ID,
            TO_DATE(PV_TS) AS starttime,
            DATEADD(day, 30, TO_DATE(PV_TS)) AS endtime
        FROM ANALYTIC_DB.DBT_marts.new_user_reattribution
        WHERE TO_DATE(PV_TS) >= '{origin_date}'
              AND TO_DATE(PV_TS) <= '{end_date}'
    ),
    advance_revenue AS
    (
        SELECT
            user_pv.starttime AS datetime,
            revenue.USER_ID,
            revenue.PLEDGED_ADVANCE_REVENUE AS total
        FROM ANALYTIC_DB.DBT_metrics.pledged_advance_revenue revenue
        JOIN user_pv
        ON revenue.USER_ID = user_pv.USER_ID
            AND revenue.DISBURSEMENT_DS_PST >= user_pv.starttime
            AND revenue.DISBURSEMENT_DS_PST <= user_pv.endtime
    ),
    o2_revenue as
    (
        SELECT
            user_pv.starttime AS datetime,
            o2.USER_ID,
            o2.PLEDGED_OVERDRAFT_TIP + o2.PLEDGED_OVERDRAFT_EXPRESS_FEE + o2.PLEDGED_OVERDRAFT_SERVICE_FEE AS total
        FROM ANALYTIC_DB.DBT_metrics.o2_revenue o2
        JOIN user_pv
        ON o2.USER_ID = user_pv.USER_ID
           AND o2.EVENT_DS >= user_pv.starttime
           AND o2.EVENT_DS <= user_pv.endtime
    ),
    total_revenue as
    (
        SELECT datetime, USER_ID, total FROM advance_revenue
        UNION
        SELECT datetime, USER_ID, total FROM o2_revenue
    )
    SELECT datetime, SUM(total) AS sumtotal
    FROM total_revenue
    GROUP BY datetime
    ORDER BY datetime;
    """
    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['date', 'PV'])
    df = df.fillna(0)
    df.to_csv(datafile_path + "total_revenue.csv")
    print("Done")


def convert_data(df):
    """
        Pivot platform_raw dataset and save the basic format for further analysis.
    """

    dfupdate = df.loc[:, ["date", "channel", "platform", "spending"]]

    # To channel and platform combination to remove the ambiguity
    dfupdate['channel'] = dfupdate['channel'].astype(str) + "_" + dfupdate["platform"]
    print(dfupdate.columns)

    df_1 = pd.pivot_table(dfupdate, columns=['channel'], index=['date'], values=['spending'], aggfunc='sum', fill_value=0)
    df_1.columns = [x[1].replace(" ", "_") for x in df_1.columns]
    df_1 = df_1.reset_index()

    df_1.to_csv(datafile_path + "channel_spending_raw.csv")
    print("convert table done")


def main():
    origin_date = '2021-01-01'
    # If use revenue as the dependent variable, we have to leave one month to collect revenue for a better estimation
    end_date = datetime.today() - timedelta(days=32)
    # define user_type to set user to advance users
    user_type = 'advance'

    df = extract_spending_data(origin_date, end_date)
    convert_data(df)

    if FLAG == 1:
        extract_user_data(origin_date, end_date, user_type)
    else:
        extract_revenue_data(origin_date, end_date)


if __name__ == '__main__':
    main()
