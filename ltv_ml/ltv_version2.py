import enum
from subprocess import call
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from ltv_version2_global import *
from udf import feature_clean, read_from_database, split_train_test_two
import pickle
from sklearn.linear_model import LinearRegression
import math
import statsmodels.api as sm
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers import Dense

def nn_model():
    model = Sequential()
    model.add(Dense(24, input_dim=21, activation='relu'))
    model.add(Dense(12, activation='linear'))
    model.add(Dense(1, activation='linear'))
    model.compile(loss='mse', optimizer='adam', metrics=['mse', 'mae'])
    return model

def train_test_data(df):
    x_value, x_name = feature_clean(df, numeric_columns, categorical_columns)
    y_value = df['revenue'].values
    x_userid = df.loc[:, ['userid', 'startdate']].values
    x_train, x_test, userid_train, userid_test, y_train, y_test = split_train_test_two(x_value, y_value, 0.25, x_userid)
    return x_name, x_train, x_test, userid_train, userid_test, y_train, y_test

def rnn_model(x_name, x_train, x_test, userid_train, userid_test, y_train, y_test, df, df_ltv):
    model = nn_model()
    model.fit(x_train, y_train, epochs=30, batch_size=75, validation_data=(x_test, y_test))
    y_predict = model.predict(x_test)

      #Test accuracy
    users_test = pd.DataFrame(userid_test, columns=['userid', 'startdate'])
    df_test_checkupdate = pd.merge(users_test, df.loc[:, ['userid', 'revenue']], on=['userid'])
    df_test_ltv = pd.merge(df_test_checkupdate, df_ltv.loc[:, ['userid', 't_value']], on=['userid'])
    df_test_ltv['month'] = df_test_ltv['startdate'].apply(lambda x: x[0:7])
    df_test_ltv['prediction'] = y_predict
    df_test_ltv['diff'] = df_test_ltv['prediction'] - df_test_ltv['revenue']

    df_test_diff = df_test_ltv.loc[:, ['userid', 'diff']]
    df_test_diff.to_csv(datafile_path + "df_test_diff_tmp.csv")

def call_model(df, df_ltv):
    x_name, x_train, x_test, userid_train, userid_test, y_train, y_test = train_test_data(df)
    rnn_model(x_name, x_train, x_test, userid_train, userid_test, y_train, y_test, df, df_ltv)
    linear_statsmodels(x_name, x_train, x_test, userid_train, userid_test, y_train, y_test, df, df_ltv)

def read_data():
    # Read Jing's query
    Jing_query = """
    select * from DBT.DEV_JGAN_PUBLIC.USER_DATA_2022 WHERE ADVANCE_TAKEN_USER=1
    """
    result = read_from_database(Jing_query)
    df = pd.DataFrame(result, columns = jing_columns)
    #Read df_x_y.csv
    df_x_y = pd.read_csv(datafile_path + "df_x_y.csv", header=0)
    df_total = pd.merge(df_x_y, df, on=['userid'])
    with open(datafile_path + "df_total.pk", 'wb') as f:
        pickle.dump(df_total, f)

def simple_output(df_test):
    monthlist = list(set(df_test['month']))
    for item in monthlist:
        tmp = df_test.loc[df_test['month'] == item,:]
        print("The month is " + item)
        print("The true revenue is " + str(tmp['revenue'].sum()))
        print("The statistical revenue is " + str(tmp['t_value'].sum()))
        print("The ML revenue is " + str(tmp['prediction'].sum()))
        print("\n")

def compare_output(p1, p2):
    monthlist = list(set(p1['month']))
    for item in monthlist:
        tmp1 = p1.loc[p1['month'] == item,:]
        tmp2 = p2.loc[p2['month'] == item,:]
        print("The month is " + item)
        print("The true revenue is " + str(tmp1['revenue'].sum()))
        print("The statistical revenue is " + str(tmp1['t_value'].sum()))
        print("The ML revenue is " + str(tmp1['prediction'].sum()))
        print("The ML revenue from statismodels is " + str(tmp2['prediction'].sum()))
        print("\n")

def linear_regression(df, df_ltv):
    x_value, x_name = feature_clean(df, numeric_columns, categorical_columns)
    y_value = df['revenue'].values
    x_userid = df.loc[:, ['userid', 'startdate']].values
    x_train, x_test, userid_train, userid_test, y_train, y_test = split_train_test_two(x_value, y_value, 0.2, x_userid)

    reg = LinearRegression()
    reg.fit(x_train, y_train)

    y_predict = reg.predict(x_test)

    #Test accuracy
    users_test = pd.DataFrame(userid_test, columns=['userid', 'startdate'])
    df_test_checkupdate = pd.merge(users_test, df.loc[:, ['userid', 'revenue']], on=['userid'])
    df_test_ltv = pd.merge(df_test_checkupdate, df_ltv.loc[:, ['userid', 't_value']], on=['userid'])
    df_test_ltv['month'] = df_test_ltv['startdate'].apply(lambda x: x[0:7])
    df_test_ltv['prediction'] = y_predict

    #simple_output(df_test_ltv)
    return df_test_ltv



def linear_statsmodels(x_name, x_train, x_test, userid_train, userid_test, y_train, y_test, df, df_ltv):

    x_train = sm.add_constant(x_train, has_constant='add')
    model = sm.OLS(y_train, x_train).fit()

    x_test = sm.add_constant(x_test, has_constant='add')
    y_predict = model.predict(x_test)
    y_train_predict = model.predict(x_train)

    #Test accuracy
    users_test = pd.DataFrame(userid_test, columns=['userid', 'startdate'])
    df_test_checkupdate = pd.merge(users_test, df.loc[:, ['userid', 'revenue']], on=['userid'])
    df_test_ltv = pd.merge(df_test_checkupdate, df_ltv.loc[:, ['userid', 't_value']], on=['userid'])
    df_test_ltv['month'] = df_test_ltv['startdate'].apply(lambda x: x[0:7])
    df_test_ltv['prediction'] = y_predict
    df_test_ltv['diff'] = df_test_ltv['prediction'] - df_test_ltv['revenue']
    df_test_diff = df_test_ltv.loc[:, ['userid', 'diff']]

    #Train accuracy
    users_train = pd.DataFrame(userid_train, columns=['userid', 'startdate'])
    df_train_checkupdate = pd.merge(users_train, df.loc[:, ['userid', 'revenue']], on=['userid'])
    df_train_ltv = pd.merge(df_train_checkupdate, df_ltv.loc[:, ['userid', 't_value']], on=['userid'])
    df_train_ltv['month'] = df_train_ltv['startdate'].apply(lambda x: x[0:7])
    df_train_ltv['prediction'] = y_train_predict
    df_train_ltv['diff'] = df_train_ltv['prediction'] - df_train_ltv['revenue']
    df_train_diff = df_train_ltv.loc[:, ['userid', 'diff']]

    #simple_output(df_test_ltv)
    print_model = model.summary()
    print(print_model)
    for i, item in enumerate(x_name):
        if item is None:
            item = "None"
        print("x" + str(i) + ":\t" + item)
    print(x_name)
    df_test_diff.to_csv(datafile_path + "df_test_diff.csv")
    df_train_diff.to_csv(datafile_path + "df_train_diff.csv")
    #return df_test_ltv

def linear_regression_monthly(df, df_ltv):
    df['month'] = df['startdate'].apply(lambda x: x[0:7])
    monthlist = ["2021-0" + str(i) for i in range(1, 10)] + ["2021-" + str(i) for i in range(10, 13)]
    for item in monthlist:
        df_tmp = df.loc[df['month'] == item, :]
        linear_regression(df_tmp, df_ltv)

def add_seasonality(df):
    df['month'] = df['startdate'].apply(lambda x: int(str(x)[5:7]))
    df['sine_month'] = df['month'].apply(lambda x: math.sin(2 * math.pi * x / 12))
    df['cosine_month'] = df['month'].apply(lambda x: math.cos(2 * math.pi * x / 12))
    return df

def main():
    #read_data()
    with open(datafile_path + "df_total.pk", 'rb') as f:
        df = pickle.load(f)
    df = df.drop_duplicates(['userid'])
    dfupdate = add_seasonality(df)
    #dfupdate = dfupdate.loc[dfupdate['revenue']<=100, :]
    print(dfupdate.shape)
    dfupdate['startmonth'] = dfupdate['startdate'].str.slice(0, 7)
    dfupdate_1 = dfupdate.loc[dfupdate['startmonth'].isin(['2021-12']),:]
    # read ltv value
    df_ltv = pd.read_csv("/Users/yanchunyang/Documents/datafiles/ltv/" + "dftotal.csv", header=0)
    print(df_ltv.shape)
    #p_model_1 = linear_regression(dfupdate, df_ltv)
    #p_model_2 = linear_statsmodels(dfupdate, df_ltv)
    #rnn_model(dfupdate, df_ltv)
    call_model(dfupdate, df_ltv)
    #compare_output(p_model_1, p_model_2)
    #linear_regression_monthly(dfupdate, df_ltv)
    print("Done")

if __name__ == '__main__':
    main()


