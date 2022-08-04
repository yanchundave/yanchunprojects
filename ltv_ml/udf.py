import numpy as np
import jaydebeapi as jay
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

def get_token(eventstr, vocab):
    splits = eventstr.strip().split(",")
    return [vocab[x] for x in splits]


def read_from_database(sql_str):
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

def feature_clean(df, numerical_columns, categorical_columns=None):
    # obtain categorical_columns
    columns_name = []
    df_cat = df.loc[:, categorical_columns]
    if categorical_columns:
        cat_encoder = OneHotEncoder(sparse=False, categories='auto')
        array_category = cat_encoder.fit_transform(df_cat)
        category_name = cat_encoder.categories_
        columns_name += [x for item in category_name for x in item]

    # obtain numerica_columns
    df_num = df.loc[:, numerical_columns]
    df_num = df_num.fillna(0)
    imputer = SimpleImputer(strategy='median')
    scaler = StandardScaler()

    x = imputer.fit_transform(df_num)
    x = scaler.fit_transform(x)
    x_combine = np.concatenate([array_category, x], axis=1)
    columns_name += numerical_columns

    return x_combine, columns_name

def split_train_test(x, y, test_ratio, xx=None):
    shuffled_indices = np.random.permutation(len(x))
    test_set_size = int(len(x) * test_ratio)
    test_indices = shuffled_indices[:test_set_size]
    train_indices = shuffled_indices[test_set_size:]
    if xx is not None:
        return x[train_indices,:,:,:], x[test_indices,:,:,:], xx[train_indices,:], xx[test_indices,:], y[train_indices,:], y[test_indices,:]
    else:
        return x[train_indices,:,:,:], x[test_indices,:,:,:], y[train_indices,:], y[test_indices,:]

