import numpy as np 
import pandas as pd
from sklearn.preprocessing import StandardScaler
from keras.layers import Dense 
from sklearn.preprocessing import LabelEncoder
from udf import read_from_database
from keras.utils import np_utils
from keras.models import Sequential
from global_variable import *


def assign_bucket(x):
    for i in range(0, len(values_bucket)):
        if values_bucket[i] >= x:
            return i
    return len(values_bucket)

def get_std(timediff_array):
    timediff_array.sort()
    length = len(timediff_array)
    if length > 1:
        tmp = [timediff_array[i] - timediff_array[i-1] for i in range(1, length)]
        return np.std(tmp)
    else:
        return 0

def get_std_str(session_str):
    splits = session_str.strip().split(",")
    if len(splits) == 1:
        return 0
    else:
        session_array = [int(x) for x in splits]
        return np.std(session_array)

def split_train_test(data, test_ratio):
    shuffled_indices = np.random.permutation(len(data))
    test_set_size = int(len(data) * test_ratio)
    test_indices = shuffled_indices[:test_set_size]
    train_indices = shuffled_indices[test_set_size:]
    return data.iloc[train_indices], data.iloc[test_indices]

def clean_dataset(train_set, x_cols):
    X = train_set.loc[:, x_cols].values
    Y = train_set['revenue_bucket']
    encoder = LabelEncoder()
    encoder_y = encoder.fit_transform(Y)
    dummy_y = np_utils.to_categorical(encoder_y)
    return X, dummy_y

def nn_model():
    model = Sequential()
    model.add(Dense(20, input_dim=12, activation='relu'))
    model.add(Dense(40, activation='relu'))
    model.add(Dense(20, activation='softmax'))
    model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    return model

def get_data_from_database():
    advance_sql = """
    SELECT USER_ID, STARTDATE, FREQUENCY, T, RECENCY, AGE, MONETARY 
    FROM DBT.DEV_YANCHUN_PUBLIC.USER_ADVANCE_FEATURE_1
    """
    advance_raw = """
    SELECT USER_ID, STARTDATE, TRANS_TIME, REVENUE
    FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
    WHERE to_date(TRANS_TIME) < '2022-01-01' 
    """

    session_sql = """
    SELECT USER_ID, LASTQUARTER_SESSION, SESSION_STR, SESSIONTOTAL, LASTSESSION, ACTIVEMONTH
    FROM DBT.DEV_YANCHUN_PUBLIC.USER_SESSION_FEATURE_2  
    """

    revenue_sql = """
    SELECT USER_ID, SUM(REVENUE) AS REVENUE
    FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
    WHERE TRANS_TIME >= DATE('2022-01-01') AND TRANS_TIME < DATE('2022-07-01')
    GROUP BY USER_ID
    """

    df_advance_feature = read_from_database(advance_sql)
    advance_feature_1 = pd.DataFrame(df_advance_feature, columns=['userid', 'startdate', 'frequency', 'T', 'recency', 'age', 'monetary'])

    df_advance_session = read_from_database(advance_raw)
    advance_session = pd.DataFrame(df_advance_session, columns=['userid', 'startdate', 'transtime', 'revenue'])
    advance_session['startdate'] = pd.to_datetime(advance_session['startdate'])
    advance_session['transtime'] = pd.to_datetime(advance_session['transtime'])
    advance_session['timediff'] = advance_session['transtime'] - advance_session['startdate']
    advance_session['timediff'] = advance_session['timediff'].apply(lambda x: x.days)
    advance_session['timediff'] = advance_session['timediff'].astype(int)
    advance_feature_2 = advance_session.groupby(['userid'])['timediff'].apply(list).reset_index()
    advance_feature_2['timediff_std'] = advance_feature_2['timediff'].apply(lambda x: get_std(x))

    advance_feature_3 = advance_session.groupby(['userid'])['revenue'].apply(list).reset_index()
    advance_feature_3['revenue_std'] = advance_feature_3['revenue'].apply(lambda x:get_std(x))

    advance_feature_2 = advance_feature_2.drop(['timediff'], axis=1)
    advance_feature_3 = advance_feature_3.drop(['revenue'], axis=1)

    advance_feature_4 = pd.merge(advance_feature_2, advance_feature_3, on=['userid'])
    advance_feature_5 = pd.merge(advance_feature_1, advance_feature_4, on=['userid'])

    df_session_raw = read_from_database(session_sql)
    session_feature = pd.DataFrame(df_session_raw, columns=['userid', 'lastquarter_session', 
            'session_str','sessiontotal', 'lastsession', 'activemonth'])
    session_feature['session_std'] = session_feature['session_str'].apply(lambda x: get_std_str(x))
    session_feature = session_feature.drop(['session_str'], axis=1)

    advance_feature_6 = pd.merge(advance_feature_5, session_feature, on=['userid'])

    df_revenue = read_from_database(revenue_sql)
    revenue_user = pd.DataFrame(df_revenue, columns=['userid', 'revenue'])

    df_x_y = pd.merge(advance_feature_6, revenue_user, on=['userid'], how='left')
    df_x_y = df_x_y.fillna(0)
    df_x_y['revenue_bucket'] = df_x_y['revenue'].apply(lambda x: assign_bucket(x))
    df_x_y.to_csv(datafile_path + "df_x_y.csv")

    return df_x_y

def model_training(df_x_y):
    x_cols = ['frequency', 'T', 'recency', 'age', 'monetary','timediff_std', 'revenue_std', 'lastquarter_session', 'sessiontotal',
       'lastsession', 'activemonth', 'session_std']
    std_scaler = StandardScaler()
    df_x_scale = std_scaler.fit_transform(df_x_y.loc[:, x_cols].values)
    df_x_scale_y = np.concatenate((df_x_scale, df_x_y['revenue_bucket'].values.reshape(-1,1)), axis=1)
    df_origin = pd.DataFrame(df_x_scale_y, columns = x_cols + ['revenue_bucket'])
    df_origin['userid'] = df_x_y['userid']
    df_origin.to_csv(datafile_path + "df_origin.csv")

    train_set, test_set = split_train_test(df_origin, 0.3)
    train_x, train_y = clean_dataset(train_set, x_cols)
    model = nn_model()
    model.fit(train_x, train_y, epochs=100, batch_size=75)

    test_x, test_y = clean_dataset(test_set, x_cols)
    predicted_y = model.predict(test_x)
    predicted_class = np.argmax(predicted_y, axis=1)
    acutal_class = test_set['revenue_bucket']

    diff_class = np.abs(predicted_class - acutal_class)
    accuracy = 1.0 * len(diff_class[diff_class <= 1]) / len(predicted_class)
    print("The accuracy is " + str(accuracy))

    test_set['predicted_class'] = predicted_class
    test_set.to_csv(datafile_path + "test_set.csv")


def main():
    #df_x_y = get_data_from_database()
    df_x_y = pd.read_csv(datafile_path + "df_x_y.csv", header=0)
    model_training(df_x_y)

if __name__ == '__main__':
    main()
