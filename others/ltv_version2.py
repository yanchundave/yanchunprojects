import enum
from subprocess import call
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from ltv_version2_global import *
from udf import split_train_test_two, read_from_database
import pickle
from sklearn.linear_model import LinearRegression
import math
import statsmodels.api as sm
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers import Dense
from sklearn.linear_model import LinearRegression, LogisticRegression

channels = ['Adwords', 'Apple Search Ads', 'Facebook', 'Organic', 'Referral', 'Snapchat', 'bytedanceglobal_int']
def feature_clean(df, numerical_columns, categorical_columns=None):
    # obtain categorical_columns
    columns_name = []
    array_category = None
    if categorical_columns:
        df_cat = df.loc[:, categorical_columns]
        for item in categorical_columns:
            df_cat[item] = df_cat[item].astype(str)
            df_cat[item].fillna(item+'_None', inplace=True)
            df_cat.loc[df_cat[item]=='None', [item]] = item + '_None'
            df_cat[item] = df_cat[item].str.upper()
    df_cat.loc[~df_cat['NETWORK'].isin(channels), ['NETWORK']] = 'NETWORK_OTHERS'
    df_cat = df_cat.fillna('unknown')
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
    if columns_name:
        x_combine = np.concatenate([array_category, x], axis=1)
    else:
        x_combine = x
    columns_name += numerical_columns

    return x_combine, columns_name

def nn_model():
    model = Sequential()
    model.add(Dense(24, input_dim=21, activation='relu'))
    model.add(Dense(12, activation='linear'))
    model.add(Dense(1, activation='linear'))
    model.compile(loss='mse', optimizer='adam', metrics=['mse', 'mae'])
    return model

def train_test_data(df):
    x_value, x_name = feature_clean(df, numeric_features, category_features)
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
    #rnn_model(x_name, x_train, x_test, userid_train, userid_test, y_train, y_test, df, df_ltv)
    linear_statsmodels(x_name, x_train, x_test, userid_train, userid_test, y_train, y_test, df, df_ltv)

def read_data():
    # Read Jing's query mainly for bank infortion
    Jing_query = """
    select * from DBT.DEV_JGAN_PUBLIC.USER_DATA_2022 WHERE ADVANCE_TAKEN_USER=1
    """
    result = read_from_database(Jing_query)
    df = pd.DataFrame(result)
    #Read df_x_y.csv (with std variation)
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
    x_value, x_name = feature_clean(df, numeric_features, category_features)
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

def logistic_regression(x_train, x_test, y_train, y_test, df_test):
    y_train_update = np.where(y_train>0, 1, -1)
    lreg = LogisticRegression(random_state=0).fit(x_train, y_train_update)
    print(lreg.classes_)
    y_predict = lreg.predict_proba(x_test)
    df_test[['PREDICT_CHURN', 'PREDICT_RETENTION']] = y_predict
    print("Logistic regression model done")
    print(np.mean(df_test['PREDICT_RETENTION']))
    print(np.mean(df_test['PREDICT_CHURN']))
    print(sum(np.where(df_test['REVENUE'] > 0, 1, 0)) / len(df_test))
    return lreg

def linear_statsmodels(x_name, x_train, x_test, userid_train, userid_test, y_train, y_test, df, df_ltv):


    x_train = sm.add_constant(x_train, has_constant='add')
    model = sm.OLS(y_train, x_train).fit()

    x_test = sm.add_constant(x_test, has_constant='add')
    y_predict = model.predict(x_test)
    y_train_predict = model.predict(x_train)

    #Test accuracy
    users_test = pd.DataFrame(userid_test, columns=['userid', 'startdate'])
    df_test_checkupdate = pd.merge(users_test, df.loc[:, ['userid', 'revenue']], on=['userid'], how='left')
    df_test_ltv = pd.merge(df_test_checkupdate, df_ltv.loc[:, ['userid', 't_value']], on=['userid'], how='left')
    df_test_ltv['month'] = df_test_ltv['startdate'].apply(lambda x: x[0:7])
    df_test_ltv['prediction'] = y_predict
    df_test_ltv['diff'] = df_test_ltv['prediction'] - df_test_ltv['revenue']
    df_test_diff = df_test_ltv.loc[:, ['userid', 'diff', 'prediction']]

    #Train accuracy
    users_train = pd.DataFrame(userid_train, columns=['userid', 'startdate'])
    df_train_checkupdate = pd.merge(users_train, df.loc[:, ['userid', 'revenue']], on=['userid'], how='left')
    df_train_ltv = pd.merge(df_train_checkupdate, df_ltv.loc[:, ['userid', 't_value']], on=['userid'], how='left')
    df_train_ltv['month'] = df_train_ltv['startdate'].apply(lambda x: x[0:7])
    df_train_ltv['prediction'] = y_train_predict
    df_train_ltv['diff'] = df_train_ltv['prediction'] - df_train_ltv['revenue']
    df_train_diff = df_train_ltv.loc[:, ['userid', 'diff', 'prediction']]

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

def update_network(df):
    df.loc[~df['NETWORK'].isin(channels), ['NETWORK']] = 'NETWORK_OTHERS'
    return df

def clean_data(df):
    x_value, x_name = feature_clean(df, numeric_features, category_features)
    y_value = df['revenue'].values
    x_userid = df.loc[:, ['userid', 'startdate']].values
    x_train, x_test, userid_train, userid_test, y_train, y_test = split_train_test_two(x_value, y_value, 0.2, x_userid)

def main():
    #read_data()
    with open(datafile_path + "longtermuserfile.pk", 'rb') as f:
        df = pickle.load(f)
    df = df.drop_duplicates(['userid'])
    dfupdate = add_seasonality(df)


    dfupdate['startmonth'] = dfupdate['startdate'].str.slice(0, 7)
    p_model_2 = linear_statsmodels(dfupdate)

    #dfupdate_1 = dfupdate.loc[dfupdate['startmonth'].isin(['2021-12']),:]
    # read ltv value
    #df_ltv = pd.read_csv("/Users/yanchunyang/Documents/datafiles/ltv/" + "dftotal.csv", header=0)
    #print(df_ltv.shape)
    #p_model_1 = linear_regression(dfupdate, df_ltv)
    #p_model_2 = linear_statsmodels(dfupdate)
    #rnn_model(dfupdate, df_ltv)
    #call_model(dfupdate, df_ltv)
    #compare_output(p_model_1, p_model_2)
    #linear_regression_monthly(dfupdate, df_ltv)
    print("Done")

if __name__ == '__main__':
    main()


