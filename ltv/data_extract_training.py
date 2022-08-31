import os
import numpy as np
import pandas as pd
from global_variable import *

SNOWFLAKE_ROLE = 'FUNC_ACCOUNTING_USER'
SNOWFLAKE_WAREHOUSE = 'DAVE_USER_WH'
con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)

def extract_advance_user():
    sql_str =  f"""
    SELECT * FROM ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022_TRAINING;
    """
    results = pd.read_sql_query(sql_str, con)
    results.columns = ['userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary']
    results.to_csv(datafile + "advance_user.csv")
    print("Advance Extract Done")

def extract_test_user():
    sql_str = f"""
    SELECT * FROM ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022_TEST;
    """
    results =  pd.read_sql_query(sql_str, con)
    results.columns = ['userid', 'trans_num', 'real_revenue']
    results.to_csv(datafile + "testdata.csv")
    print("Test data done")

def extract_all_pv():
    sql_str = f"""
    SELECT * FROM ACCOUNTING.DBT_LOCAL.LTV_USER_2022 WHERE startdate < date('2022-01-01') ;
    """
    results = pd.read_sql_query(sql_str, con)
    results.columns = ['userid', 'startdate', 'platform', 'attribution', 'network']
    results.to_csv(datafile + "users_property.csv")


def main():
    extract_advance_user()
    extract_test_user()
    extract_all_pv()

if __name__ == '__main__':
    main()