import jaydebeapi as jay
import os
import numpy as np 
import pandas as pd 
#import plotly.express as px

datafile = "/Users/yanchunyang/Documents/datafiles/"
channels = ['Adwords', 'Apple Search Ads', 'BRANDING', 'Facebook', 'Reddit', 'Snapchat', 'Tatari', 'Videoamp', 'bytedanceglobal_int']

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

def draw_graph(df):
    #fig = px.line(df, )
    pass

def extract_spending_data():
    sql_str = """
    select date(SPEND_DATE_PACIFIC_TIME), network, PLATFORM, sum(spend)
    from ANALYTIC_DB.DBT_MARTS.MARKETING_SPEND 
    where SPEND_DATE_PACIFIC_TIME>='2022-01-01'
    AND SPEND_DATE_PACIFIC_TIME <= '2022-06-04'
    group by 1, 2, 3
    order by 1, 2, 3;
    """
    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['date', 'channel', 'platform', 'spending'])
    values = {'platform': 'unknown', 'spending':0}
    df = df.fillna(value=values)
    df.to_csv(datafile + "platform_raw.csv")

def extract_user_data():
    sql_str = """
    SELECT 
    LEFT(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz) , 10) AS date, 
    SUM(case when PV_TS is not null then 1 else 0 end) AS PV
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='2022-01-01'
    AND to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))<='2022-06-04'
    GROUP BY 1
    ORDER BY 1;
    """
    sql_str_advance = """
    SELECT 
    LEFT(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz) , 10) AS date, 
    SUM(case when ADVANCE_TAKEN_USER is not null then 1 else 0 end) AS PV
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz)) >='2022-01-01'
    AND to_date(convert_timezone('UTC','America/Los_Angeles',PV_TS::timestamp_ntz))<='2022-06-04'
    GROUP BY 1
    ORDER BY 1;
    """
    #results = read_from_snowflake(sql_str_advance)
    results = read_from_snowflake(sql_str_advance)
    df = pd.DataFrame(results, columns=['date', 'PV'])
    df = df.fillna(0)
    df.to_csv(datafile + "platform_user_advance.csv")
    print("Done")

def extract_revenue_data():
    sql_str = """
    SELECT EVENT_DS, SUM(TOTAL_DAVE_REVENUE)
    FROM ANALYTIC_DB.DBT_METRICS.TOTAL_DAVE_REVENUE
    where EVENT_DS >='2022-01-01'
    and EVENT_DS <='2022-06-04'
    GROUP BY EVENT_DS
    ORDER BY EVENT_DS;
    """
    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['date', 'PV'])
    df = df.fillna(0)
    df.to_csv(datafile + "total_revenue.csv")
    print("Done")

def pivot_table(df, network):
    dfupdate = pd.pivot_table(df, columns=['platform'], index=['date'], 
        values=['spending'], aggfunc='sum', fill_value=0)
    dfupdate.columns = [network + '_' + x[0] + '_' + x[1] for x in dfupdate.columns]
    return dfupdate  

def convert_data():
    df = pd.read_csv(datafile + "platform_raw.csv", header=0)
    dfupdate = df.loc[:, ["date", "channel", "platform", "spending"]]
    dfupdate['channel_update'] = dfupdate['channel'].astype(str) + "_" + dfupdate["platform"]
    dfupdate['channel'] = dfupdate['channel_update'].str.replace(" ", "_")
    print(dfupdate.columns)
    df_1 = pd.pivot_table(dfupdate, columns=['channel'], index=['date'], values=['spending'], aggfunc='sum', fill_value=0)
    df_1.columns = [x[1] for x in df_1.columns]
    df_1 = df_1.reset_index()
    df_1.to_csv(datafile + "channel_spending_raw.csv")
    """
    results = []
    for i, item in enumerate(channels):
        results.append(pivot_table(df.loc[df['channel'] == item, :], item))
    df_update = pd.concat(results, axis=1)
    df_update = df_update.reset_index()
    df_update.to_csv(datafile + "channel_spending_raw.csv")
    """

def main():
    #extract_spending_data()
    #convert_data()
    #extract_user_data()
    extract_revenue_data()

if __name__ == '__main__':
    main()
