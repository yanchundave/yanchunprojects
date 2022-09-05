import enum
from subprocess import call
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from udf import feature_clean, read_from_database, split_train_test_two
import pickle
from sklearn.linear_model import LinearRegression
import math
import statsmodels.api as sm
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers import Dense
from global_variable import *
from new_user_global import *


FORECAST_DATE = '2022-01-01'
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

def linear_regression(df_train, df_test):
    columns = df_train.columns
    x_columns = columns[0: -4]

    x_train = df_train.loc[:, x_columns].values
    y_train = df_train['REVENUE']
    x_test = df_test.loc[:, x_columns].values
    y_test = df_test['REVENUE']

    reg = LinearRegression()
    reg.fit(x_train, y_train)

    y_predict = reg.predict(x_test)
    df_test['PREDICT'] = y_predict
    print(np.mean(df_test['REVENUE']))
    print(np.mean(df_test['PREDICT']))

def get_latest_arpu(month_arpu):
    month_arpu['lag_1'] = month_arpu['arpu'].shift(1)
    month_arpu['lag_2'] = month_arpu['arpu'].shift(2)
    month_arpu['lag_3'] = month_arpu['arpu'].shift(3)
    month_1 = month_arpu.loc[:, ['month', 'lag_1', 'lag_2', 'lag_3']]
    month_1.columns = ['FORECASTDATE', 'LAG1', 'LAG2', 'LAG3']
    return month_1

def obtain_data():
    with open(datafile_path + "newuserfile.pk", 'rb') as f:
        df = pickle.load(f)
    month_arpu = pd.read_csv(datafile_path + "month_arpu.csv", header=0)
    latest_arpu = get_latest_arpu(month_arpu)
    print(df.shape)
    dfupdate = df.loc[df['FIRST_TRANS'].notnull(), :]
    dfupdate['monthnumber'] = dfupdate['STARTDATE'].str.slice(5, 7).astype(int)
    dfupdate['SINE_MONTH'] = np.sin(2 * math.pi * dfupdate['monthnumber'] / 12)
    dfupdate['COS_MONTH'] = np.cos(2 * math.pi * dfupdate['monthnumber'] / 12)
    dfupdate['STARTMONTH'] = dfupdate['STARTDATE'].str.slice(0, 7)
    dfupdate_1 = pd.merge(dfupdate, latest_arpu, on=['FORECASTDATE'], how='left')

    x_value, x_name = feature_clean(dfupdate_1, numeric_features, category_features)
    x_userid = dfupdate.loc[:, ['USER_ID', 'STARTDATE', 'STARTMONTH', 'REVENUE']].values
    x_value = np.concatenate([x_value, x_userid], axis=1)
    df_v = pd.DataFrame(x_value, columns = x_name + ['USER_ID', 'STARTDATE', 'STARTMONTH', 'REVENUE'])
    df_v = df_v.fillna(0)

    df_train = df_v.loc[~df_v['STARTMONTH'].isin(['2021-10', '2021-11', '2021-12']), :]
    df_test = df_v.loc[df_v['STARTMONTH'].isin(['2021-10', '2021-11', '2021-12']), :]
    print(df_train.columns)
    return df_train, df_test


def main():
    df_train, df_test = obtain_data()
    linear_regression(df_train, df_test)


if __name__ == '__main__':
    main()