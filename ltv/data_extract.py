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
    warehouse = "DAVE_USER_WH"
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

def extract_advance_user():
    sql_str =  f"""
    with USER_TRANS AS 
    (
        SELECT 
        USER_ID, 
        TRANS_TIME,
        TRANS_ID,
        REVENUE
        FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
        WHERE to_date(TRANS_TIME) <= '{training_end_date}'
    )
    SELECT 
    USER_ID, 
    DATE(MIN(TRANS_TIME)) AS first_trans,
    COUNT(TRANS_ID) - 1 AS frequency, 
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('{test_start_date}')) AS T, 
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS recency,
    AVG(REVENUE) AS monetary
    FROM USER_TRANS
    GROUP BY USER_ID;
    """
    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary'])
    df.to_csv(datafile + "advance_user.csv")
    print("Advance Extract Done")

def extract_test_user():
    sql_str = f"""
    SELECT USER_ID, COUNT(TRANS_ID) AS trans_num, SUM(REVENUE) AS real_revenue
    FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
    WHERE to_date(TRANS_TIME) >= date('{test_start_date}')
    GROUP BY USER_ID
    """
    results = read_from_snowflake(sql_str)
    df = pd.DataFrame(results, columns=['userid', 'trans_num', 'real_revenue'])
    df.to_csv(datafile + "testdata.csv")
    print("Test data done")


def main():
    extract_advance_user()
    extract_test_user()

if __name__ == '__main__':
    main()