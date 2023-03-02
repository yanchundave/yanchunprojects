import pandas as pd
from udf import read_from_database
from global_variable import *
import pickle


def read_data():
    suffix = ['ACCOUNTING.DBT_LOCAL.LTV_USER_LONGTERM', 'ACCOUNTING.DBT_LOCAL.LTV_USER_LONGTERM_CANDIDATE']
    columns_name = ['USER_ID', 'STARTDATE', 'PLATFORM', 'ATTRIBUTION', 'NETWORK', 'BOD_ACCOUNT_OPEN_USER',
    'BOD_DIRECT_DEPOSIT_USER', 'BANK_CATEGORY', 'HAS_VALID_CREDENTIALS', 'MOST_RECENT_REQUEST_DECLINE',
    'ADVANCE_TAKEN_AMOUNT', 'FIRST_TRANS', 'FREQUENCY', 'T', 'RECENCY', 'MONETARY',
    'TRANS_LIST', 'MONETARY_LIST', 'SESSIONTOTAL', 'SESSION_LIST','REVENUE', 'FORECASTDATE']
    arrayname = ['longterm_training.pk', 'longterm_forecast.pk']
    for i, item in enumerate(suffix):
        sql_query = f"""
        SELECT
        USER_ID,
        STARTDATE,
        PLATFORM,
        ATTRIBUTION,
        NETWORK,
        BOD_ACCOUNT_OPEN_USER,
        BOD_DIRECT_DEPOSIT_USER,
        BANK_CATEGORY,
        HAS_VALID_CREDENTIALS,
        MOST_RECENT_REQUEST_DECLINE,
        ADVANCE_TAKEN_AMOUNT,
        FIRST_TRANS,
        FREQUENCY,
        T,
        RECENCY,
        MONETARY,
        TRANS_LIST,
        MONETARY_LIST,
        SESSIONTOTAL,
        SESSION_LIST,
        REVENUE,
        FORECASTDATE
        FROM {item}
        WHERE FIRST_TRANS IS NOT NULL
        """
        result = read_from_database(sql_query, role="FUNC_ACCOUNTING_USER")
        df = pd.DataFrame(result, columns=columns_name)
        with open(datafile_path + arrayname[i], 'wb') as f:
            pickle.dump(df, f)
    print("Done")

read_data()

