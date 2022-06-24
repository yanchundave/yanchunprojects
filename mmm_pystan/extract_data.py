import jaydebeapi as jay
import os
import numpy as np 
import pandas as pd 
from global_variable import *


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


def extract_spending_data():
    sql_str = f"""
    select date(SPEND_DATE_PACIFIC_TIME), network, PLATFORM, sum(spend)
    from ANALYTIC_DB.DBT_MARTS.MARKETING_SPEND 
    where SPEND_DATE_PACIFIC_TIME>='{origin_date}'
    AND SPEND_DATE_PACIFIC_TIME <= '{end_date}'
    group by 1, 2, 3
    order by 1, 2, 3;
    """
    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['date', 'channel', 'platform', 'spending'])
    values = {'platform': 'unknown', 'spending':0}
    df = df.fillna(value=values)
    df.to_csv(datafile_path + "platform_raw.csv")

def extract_user_data():
    sql_str_onedave = f"""
    SELECT 
    LEFT(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz) , 10) AS date, 
    SUM(case when PV_TS is not null then 1 else 0 end) AS PV
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='{origin_date}'
    AND to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))<='{end_date}'
    GROUP BY 1
    ORDER BY 1;
    """
    sql_str_advance =f"""
    SELECT 
    LEFT(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz) , 10) AS date, 
    SUM(case when ADVANCE_TAKEN_USER is not null then 1 else 0 end) AS PV
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='{origin_date}'
    AND to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))<='{end_date}'
    GROUP BY 1
    ORDER BY 1;
    """
    sql_str = sql_str_advance if user_type == 'advance' else sql_str_onedave
    results = read_from_snowflake(sql_str)
        
    df = pd.DataFrame(results, columns=['date', 'PV'])
    df = df.fillna(0)
    df.to_csv(datafile_path + "platform_user_advance.csv")
    print("Done")

def extract_revenue_data():
    sql_str = f"""
    with user_pv as 
    (
    SELECT USER_ID, to_date(PV_TS) as starttime, dateadd(day, 30, to_date(PV_TS)) as endtime
    FROM ANALYTIC_DB.DBT_marts.new_user_reattribution 
    WHERE to_date(PV_TS) >= '{origin_date}' 
    AND to_date(PV_TS) <= '{end_date}'
    ),
    advance_revenue as 
    (
    SELECT user_pv.starttime as datetime, revenue.USER_ID, PLEDGED_ADVANCE_REVENUE as total
    FROM ANALYTIC_DB.DBT_metrics.pledged_advance_revenue revenue
    JOIN user_pv 
    ON revenue.USER_ID = user_pv.USER_ID
    AND revenue.DISBURSEMENT_DS_PST >= user_pv.starttime and revenue.DISBURSEMENT_DS_PST <= user_pv.endtime
    ), 
    o2_revenue as 
    (
    SELECT user_pv.starttime as datetime, o2.USER_ID, PLEDGED_OVERDRAFT_TIP + PLEDGED_OVERDRAFT_EXPRESS_FEE + PLEDGED_OVERDRAFT_SERVICE_FEE as total
    FROM ANALYTIC_DB.DBT_metrics.o2_revenue o2
    JOIN user_pv
    ON o2.USER_ID = user_pv.USER_ID
    AND o2.EVENT_DS >= user_pv.starttime AND o2.EVENT_DS <= user_pv.endtime  
    ),
    total_revenue as 
    (
    SELECT datetime, USER_ID, total FROM advance_revenue
    UNION
    SELECT datetime, USER_ID, total FROM o2_revenue
    )
    SELECT datetime, SUM(total) as sumtotal
    FROM total_revenue
    GROUP BY datetime
    ORDER BY datetime;
    """
    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['date', 'PV'])
    df = df.fillna(0)
    df.to_csv(datafile_path + "total_revenue.csv")
    print("Done")

def pivot_table(df, network):
    dfupdate = pd.pivot_table(df, columns=['platform'], index=['date'], 
        values=['spending'], aggfunc='sum', fill_value=0)
    dfupdate.columns = [network + '_' + x[0] + '_' + x[1] for x in dfupdate.columns]
    return dfupdate  

def convert_data():
    df = pd.read_csv(datafile_path + "platform_raw.csv", header=0)
    dfupdate = df.loc[:, ["date", "channel", "platform", "spending"]]
    dfupdate['channel_update'] = dfupdate['channel'].astype(str) + "_" + dfupdate["platform"]
    dfupdate['channel'] = dfupdate['channel_update'].str.replace(" ", "_")
    print(dfupdate.columns)
    df_1 = pd.pivot_table(dfupdate, columns=['channel'], index=['date'], values=['spending'], aggfunc='sum', fill_value=0)
    df_1.columns = [x[1] for x in df_1.columns]
    df_1 = df_1.reset_index()
    df_1.to_csv(datafile_path + "channel_spending_raw.csv")
    """
    results = []
    for i, item in enumerate(channels):
        results.append(pivot_table(df.loc[df['channel'] == item, :], item))
    df_update = pd.concat(results, axis=1)
    df_update = df_update.reset_index()
    df_update.to_csv(datafile + "channel_spending_raw.csv")
    """

def main():
    extract_spending_data()
    convert_data()
    if flag == 1:
        extract_user_data()
    else:
        extract_revenue_data()

if __name__ == '__main__':
    main()
