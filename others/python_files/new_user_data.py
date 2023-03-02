import pandas as pd
from udf import read_from_database
from global_variable import *
import pickle


def read_data():
    suffix = ['04', '07', '10', '12', 'CANDIDATE']
    columns_name = ['USER_ID', 'STARTDATE', 'PLATFORM', 'ATTRIBUTION', 'NETWORK', 'BOD_ACCOUNT_OPEN_USER',
    'BOD_DIRECT_DEPOSIT_USER', 'BANK_CATEGORY', 'HAS_VALID_CREDENTIALS', 'MOST_RECENT_REQUEST_DECLINE',
    'ADVANCE_TAKEN_AMOUNT', 'FIRST_TRANS', 'FREQUENCY', 'T', 'RECENCY', 'MONETARY', 'ACTIVESESSION', 'REVENUE', 'FORECASTDATE']
    dfarray = []
    for item in suffix:
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
        ACTIVESESSION,
        REVENUE,
        FORECASTDATE
        FROM ACCOUNTING.DBT_LOCAL.LTV_USER_NEWQUARTER_{item}
        WHERE FIRST_TRANS IS NOT NULL
        """
        result = read_from_database(sql_query, role="FUNC_ACCOUNTING_USER")
        df = pd.DataFrame(result, columns=columns_name)
        dfarray.append(df)
    dftotal = pd.concat(dfarray, axis=0)
    print(dftotal.shape)
    with open(datafile_path + "newuserfile.pk", 'wb') as f:
        pickle.dump(dftotal, f)
    print("Done")

# PROVIDE THE MONTHLY ARPU VALUE WHICH ARE THE FEATURES FOR THE MODEL
def arpu():
    sql_str = """
    SELECT
    SUBSTR(STARTDATE, 1, 7) AS MONTH,
    SUM(REVENUE) / COUNT(DISTINCT USER_ID) AS ARPU
    FROM ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022
    WHERE SUBSTR(STARTDATE, 1, 7) = SUBSTR(TRANS_TIME, 1, 7)
    GROUP BY SUBSTR(STARTDATE, 1, 7)
    ORDER BY SUBSTR(STARTDATE, 1, 7);
    """
    result = read_from_database(sql_str, role='FUNC_ACCOUNTING_USER')
    df = pd.DataFrame(result, columns=['month', 'arpu'])
    print(df.shape)
    df.to_csv(datafile_path + "month_arpu.csv")

read_data()
arpu()
