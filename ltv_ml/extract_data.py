from __future__ import generators    # needs to be at the top of your module
import jaydebeapi as jay
import os
import numpy as np 
import pandas as pd 


datafile_path = "/Users/yanchunyang/Documents/datafiles/ltv_ml"

def ResultIter(cursor, arraysize=10000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        yield results

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
    #result = curs.fetchall()
    i = 0
    for row in ResultIter(curs):
        i += 1
        df = pd.DataFrame(row, columns=['USER_ID', 'SESSION_ID', 'DATETIME', 'EVENT_STR'])
        df.to_csv(datafile_path + str(i) + "_event_history.csv")
        if i % 100 == 1:
            print(i)
  

def extract_data():
    sql_str =  """
    SELECT * FROM DBT.DEV_YANCHUN_PUBLIC.EVENT_LIST
    """
    read_from_snowflake(sql_str)
    print("Event History Done")

def main():
    extract_data()

if __name__ == '__main__':
    main()