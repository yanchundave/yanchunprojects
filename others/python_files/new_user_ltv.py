import enum
from subprocess import call
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from udf import feature_clean, read_from_database, split_train_test_two
import pickle
from sklearn.linear_model import LinearRegression, LogisticRegression
import math
import statsmodels.api as sm
from keras.utils import np_utils
from keras.models import Sequential
from keras.layers import Dense
from global_variable import *
from new_user_global import *


FORECAST_DATE = '2021-12-01'
channels = ['Adwords', 'Apple Search Ads', 'Facebook', 'Organic', 'Referral', 'Snapchat', 'bytedanceglobal_int']
TRAINNING_MONTH = ['2021-0' + str(x) for x in range(1, 10)]
print(TRAINNING_MONTH)
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
    model.add(Dense(32, input_dim=27, activation='relu'))
    model.add(Dense(16, activation='linear'))
    model.add(Dense(1, activation='linear'))
    model.compile(loss='mse', optimizer='adam', metrics=['mse', 'mae'])
    return model

def ann_model( x_train, x_test,  y_train, y_test, df_test):
    model = nn_model()
    model.fit(x_train, y_train, epochs=30, batch_size=75, validation_data=(x_test, y_test))
    model.save(datafile_path + "ltv_ann.h5")
    print("model saved")
    y_predict = model.predict(x_test)
    df_test['PREDICT'] = y_predict
    print("nn model result is ")
    print(np.mean(y_test))
    print(np.mean(df_test['PREDICT']))
    return model


def linear_regression(x_train, x_test, y_train, y_test):

    reg = LinearRegression()
    reg.fit(x_train, y_train)

    y_predict = reg.predict(x_test)
    print("linear regression model result")
    print(np.mean(y_test))
    print(np.mean(y_predict))

    return reg

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

    df_train = df_v.loc[df_v['STARTMONTH'].isin(TRAINNING_MONTH), :]
    df_test = df_v.loc[df_v['STARTMONTH'].isin(['2021-10', '2021-11', '2021-12']), :]
    df_candidate = df_v.loc[df_v['STARTMONTH'].isin(['2022-06', '2022-07', '2022-08']), :]
    print(df_train.columns)
    return df_train, df_test, df_candidate

def prepare_data(df_train, df_test):
    columns = df_train.columns
    x_columns = columns[0: -4]

    x_train = df_train.loc[:, x_columns].values
    y_train = df_train['REVENUE'].values
    x_test = df_test.loc[:, x_columns].values
    y_test = df_test['REVENUE'].values
    return x_train, x_test, y_train, y_test

def model_predict(reg_model, log_model, df_candidate, model_name):
    columns = df_candidate.columns
    x_columns = columns[0: -4]

    x_value = df_candidate.loc[:, x_columns].values
    df_candidate['PREDICT'] = reg_model.predict(x_value)
    df_candidate[['PREDICT_CHURN', 'PREDICT_RETENTION']] = log_model.predict_proba(x_value)
    df_candidate['LTV'] = np.where(df_candidate['PREDICT_RETENTION'] > 0, df_candidate['PREDICT'] / df_candidate['PREDICT_CHURN'], 0)
    df_candidate.to_csv(datafile_path + model_name + "_newuser_predict.csv")

def main():
    df_train, df_test, df_candidate = obtain_data()
    x_train, x_test, y_train, y_test = prepare_data(df_train, df_test)
    reg_model = linear_regression(x_train, x_test, y_train, y_test)
    lreg_model = logistic_regression(x_train, x_test, y_train, y_test, df_test)
    model_predict(reg_model, lreg_model, df_candidate, 'regression')
    # model analysis need combine another sql about user happened revenue
    #model = ann_model(x_train, x_test, y_train, y_test, df_test)
    #model_predict(model, df_candidate, 'ann')

    print("Prediction Completion")



if __name__ == '__main__':
    main()