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

def split_train_test(df, ratio):
    shuffle_index = np.random.permutation(len(df))
    test_size = int(len(df) * ratio)
    test_index = shuffle_index[:test_size]
    train_index = shuffle_index[test_size:]
    return df.iloc[train_index], df.iloc[test_index]

def category_clean(df):
    for item in category_features:
        df[item] = df[item].astype(str)
        df[item].fillna(item+'_None', inplace=True)
        df.loc[df[item]=='None', [item]] = item + '_None'
        df[item] = df[item].str.upper()
    return df

def numeric_clean(df):
    imputer = SimpleImputer(strategy='median')
    x = imputer.fit_transform(df)
    return x

def feature_clean_update(df):
    df_cat = df.loc[:, category_features]
    df_cat_update = category_clean(df_cat)
    df_num = df.loc[:, numeric_features]
    x_num = numeric_clean(df_num)

    return df_cat_update, x_num

def transform_x(df_cat_update, x_num):
    cat_encoder = OneHotEncoder(sparse=False, categories='auto')
    cat_encoder.fit(df_cat_update)
    array_category = cat_encoder.transform(df_cat_update)
    category_name = cat_encoder.categories_
    columns_name = [x for item in category_name for x in item]

    scaler = StandardScaler()
    scaler.fit(x_num)
    x = scaler.transform(x_num)

    columns_name += numeric_features
    x_combine = np.concatenate([array_category, x], axis=1)
    return scaler, cat_encoder, x_combine, columns_name

def reg_model_analysis(y, y_predict):
    print("arpu test")
    print(np.mean(y))
    print(np.mean(y_predict))

def lreg_model_analysis(y, y_predict):
    print("retention rate")
    print(len(y[y==1])/ len(y))
    print(np.mean(y_predict[:,1]))

def train_model(df):
    #FOUND 968 duplication due to different bank category
    df = df.drop_duplicates(subset=['USER_ID'])
    df = df.fillna({'REVENUE':0})
    df['LG_REVENUE'] = df['REVENUE'].apply(lambda x: 1 if x > 0 else -1)
    df_train, df_test = split_train_test(df, 0.2)

    df_train_cat, np_train_numeric = feature_clean_update(df_train)
    scaler, cat_encoder, x_combine, x_name = transform_x(df_train_cat, np_train_numeric)
    y_train = df_train['REVENUE']
    y_train_lg = df_train['LG_REVENUE']

    x_combine = sm.add_constant(x_combine, has_constant='add')
    reg_model = sm.OLS(y_train, x_combine).fit()

    df_test_cat, np_test_numeric = feature_clean_update(df_test)
    array_test_category = cat_encoder.transform(df_test_cat)
    x_test = scaler.transform(np_test_numeric)
    x_test_combine = np.concatenate([array_test_category, x_test], axis=1)
    x_test_combine =sm.add_constant(x_test_combine)
    y_test = df_test['REVENUE']
    y_test_lg = df_test['LG_REVENUE']


    y_test_predict = reg_model.predict(x_test_combine)
    reg_model_analysis(y_test, y_test_predict)

    lreg_model = LogisticRegression(random_state=0).fit(x_combine, y_train_lg)
    y_test_lg_pred = lreg_model.predict_proba(x_test_combine)
    lreg_model_analysis(y_test_lg, y_test_lg_pred)

    return scaler, cat_encoder, reg_model, lreg_model


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
    model.add(Dense(36, input_dim=35, activation='relu'))
    model.add(Dense(18, activation='linear'))
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
    df['MONTH'] = df['STARTDATE'].apply(lambda x: int(str(x)[5:7]))
    df['SINE_MONTH'] = df['MONTH'].apply(lambda x: math.sin(2 * math.pi * x / 12))
    df['COSINE_MONTH'] = df['MONTH'].apply(lambda x: math.cos(2 * math.pi * x / 12))
    return df

def calculate_std(item):
    if item is None:
        return -1
    splits = item.strip().split(",")
    if len(splits) >= 2:
        values = [float(x) for x in splits]
        return np.std(values) / np.mean(values)
    else:
        return 0

def calculate_std_derived(item):
    if item is None:
        return -1
    splits = item.strip().split(",")
    if len(splits) >= 3:
        values = [float(x) for x in splits]
        values.sort()
        values_update = [y - x for x, y in zip(values, values[1: ])]
        return np.std(values_update) / np.mean(values_update)

    else:
        return 0

def calculate_listlength(item):
    if item is None:
        return -1
    splits = item.strip().split(",")
    return len(splits)

"""
def clean_data(df):
    x_value, x_name = feature_clean(df, numeric_features, category_features)
    y_value = df['revenue'].values
    x_userid = df.loc[:, ['userid', 'startdate']].values
    x_train, x_test, userid_train, userid_test, y_train, y_test = split_train_test_two(x_value, y_value, 0.2, x_userid)
"""

def feature_derived(df):
    df = add_seasonality(df)
    df.loc[~df['NETWORK'].isin(channels), ['NETWORK']] = 'NETWORK_OTHERS'
    df['TIMEDIFF_STD'] = df['TRANS_LIST'].apply(lambda x: calculate_std_derived(x))
    df['MONETARY_STD'] = df['MONETARY_LIST'].apply(lambda x: calculate_std(x))
    df['SESSION_STD'] = df['SESSION_LIST'].apply(lambda x: calculate_std(x))
    df['ACTIVEMONTH'] = df['SESSION_LIST'].apply(lambda x: calculate_listlength(x))
    return df


def read_data():
    with open(datafile_path + "longterm_training.pk", 'rb') as f:
        df_train = pickle.load(f)
    df_train_update = feature_derived(df_train)

    with open(datafile_path + "longterm_forecast.pk", 'rb') as f:
        df_forecast = pickle.load(f)
    df_forecast_update = feature_derived(df_forecast)

    return df_train_update, df_forecast_update


def dump_pickle(df, dfname):
    with open(datafile_path + dfname, "wb") as f:
        pickle.dump(df, f)


def load_pickle(dfname):
    with open(datafile_path + dfname, "rb") as f:
        df = pickle.load(f)
    return df

def forecast_model(scaler, onehotconverter, reg_model, lreg_model,  df):
    df = df.drop_duplicates(subset=['USER_ID'])

    df_cat, np_numeric = feature_clean_update(df)
    scaler_new, cat_encoder_new, x_combine, x_name = transform_x(df_cat, np_numeric)
    x_combine =sm.add_constant(x_combine)

    y_predict = reg_model.predict(x_combine)
    print("forecast arpu is")
    print(np.mean(y_predict))

    array_test_category = onehotconverter.transform(df_cat)
    x = scaler.transform(np_numeric)
    x_combine_lg = np.concatenate([array_test_category, x], axis=1)
    x_combine_lg = sm.add_constant(x_combine_lg, has_constant='add')
    y_lg_pred = lreg_model.predict_proba(x_combine_lg)
    print("forecast retention is ")
    print(np.mean(y_lg_pred[:,1]))
    df['ARPU_PREDICT'] = y_predict
    df['RETENTION_PREDICT'] = y_lg_pred[:,1]
    return df

def forecast_rnn_model(scaler, onehotconverter, rnn_model, lreg_model,  df):
    df = df.drop_duplicates(subset=['USER_ID'])

    df_cat, np_numeric = feature_clean_update(df)
    array_test_category = onehotconverter.transform(df_cat)
    x = scaler.transform(np_numeric)
    x_combine = np.concatenate([array_test_category, x], axis=1)

    y_predict = rnn_model.predict(x_combine)
    print("forecast arpu is")
    print(np.mean(y_predict))

    y_lg_pred = lreg_model.predict_proba(x_combine)
    print("forecast retention is ")
    print(np.mean(y_lg_pred[:,1]))
    df['ARPU'] = y_predict
    df['RETENTION'] = y_lg_pred[:,1]
    return df

def train_rnn_model(df):
    #FOUND 968 duplication due to different bank category
    df = df.drop_duplicates(subset=['USER_ID'])
    df = df.fillna({'REVENUE':0})
    df['LG_REVENUE'] = df['REVENUE'].apply(lambda x: 1 if x > 0 else -1)
    df_train, df_test = split_train_test(df, 0.2)

    df_train_cat, np_train_numeric = feature_clean_update(df_train)
    scaler, cat_encoder, x_combine, x_name = transform_x(df_train_cat, np_train_numeric)
    y_train = df_train['REVENUE']
    y_train_lg = df_train['LG_REVENUE']

    df_test_cat, np_test_numeric = feature_clean_update(df_test)
    array_test_category = cat_encoder.transform(df_test_cat)
    x_test = scaler.transform(np_test_numeric)
    x_test_combine = np.concatenate([array_test_category, x_test], axis=1)
    y_test = df_test['REVENUE']
    y_test_lg = df_test['LG_REVENUE']

    rnn_model = nn_model()
    rnn_model.fit(x_combine, y_train, epochs=30, batch_size=75, validation_data=(x_test_combine, y_test))
    y_test_pred = rnn_model.predict(x_test_combine)
    reg_model_analysis(y_test, y_test_pred)

    lreg_model = LogisticRegression(random_state=0).fit(x_combine, y_train_lg)
    y_test_lg_pred = lreg_model.predict_proba(x_test_combine)
    lreg_model_analysis(y_test_lg, y_test_lg_pred)

    return scaler, cat_encoder, rnn_model, lreg_model

def main():
    #dftrain, dfforecast = read_data()
    #dump_pickle(dftrain, "dftrain.pk")
    #dump_pickle(dfforecast, "dfforecast.pk")

    dftrain = load_pickle("dftrain.pk")
    dfforecast = load_pickle("dfforecast.pk")

    scaler, onehotconverter, reg_model, lreg_model = train_model(dftrain)
    dfforecast_update = forecast_model(scaler, onehotconverter, reg_model, lreg_model,  dfforecast)
    dfforecast_update.to_csv(datafile_path + "dfforecast_update.csv")
    """
    scaler, onehotconverter, rnn_model, lreg_model = train_rnn_model(dftrain)
    dfforecast_update = forecast_rnn_model(scaler, onehotconverter, rnn_model, lreg_model,  dfforecast)
    dfforecast_update.to_csv(datafile_path + "dfforecast_update_1.csv")
    """
    print("Done")

if __name__ == '__main__':
    main()


