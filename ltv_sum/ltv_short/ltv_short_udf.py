import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder, MinMaxScaler, StandardScaler
from sklearn.linear_model import LinearRegression, LogisticRegression
from ltv_short_global import *
import math
import statsmodels.api as sm


def category_clean(df):
    for item in category_features:
        df[item] = df[item].astype(str)
        df[item].fillna(item+'_None', inplace=True)
        df.loc[df[item]=='None', [item]] = item + '_None'
        df[item] = df[item].str.upper()
    return df


def get_latest_arpu(month_arpu):
    month_arpu['FORECASTDATE'] = month_arpu['MONTH']
    month_arpu['LAG1'] = month_arpu['ARPU'].shift(1)
    month_arpu['LAG2'] = month_arpu['ARPU'].shift(2)
    month_arpu['LAG3'] = month_arpu['ARPU'].shift(3)
    return month_arpu.loc[:, ['FORECASTDATE', 'LAG1', 'LAG2', 'LAG3']]


def feature_derived(df):
    #month_arpu = get_latest_arpu(arpu)
    df['LG_REVENUE'] = df['REVENUE'].apply(lambda x: 1 if x > 0 else 0)
    df.loc[~df['NETWORK'].isin(channels), ['NETWORK']] = 'NETWORK_OTHERS'
    #network organice, referral comflicts with attribution's organic and referral
    df['NETWORK'] = df['NETWORK'].apply(lambda x: x + '_NETWORK')
    df['MONTHNUMBER'] = df['STARTDATE'].apply(lambda x: int(str(x)[5:7]))
    df['SINE_MONTH'] = np.sin(2 * math.pi * df['MONTHNUMBER'] / 12)
    df['COS_MONTH'] = np.cos(2 * math.pi * df['MONTHNUMBER'] / 12)
    df = category_clean(df)
    df = df.fillna(0)
    return df


def transform_x(df):

    df_cat = df.loc[:, category_features]
    df_num = df.loc[:, numeric_features]

    cat_encoder = OneHotEncoder(sparse=False, categories='auto')
    cat_encoder.fit(df_cat)
    array_category = cat_encoder.transform(df_cat)
    category_name = cat_encoder.categories_
    columns_name = [x for item in category_name for x in item]

    scaler = StandardScaler()
    scaler.fit(df_num.values)
    x = scaler.transform(df_num.values)

    columns_name += numeric_features
    x_combine = np.concatenate([array_category, x, df.loc[:, ['REVENUE', 'LG_REVENUE', 'FORECASTDATE']].values], axis=1)
    dfupdate = pd.DataFrame(x_combine, index=df['USER_ID'], columns=columns_name + ['REVENUE', 'LG_REVENUE', 'FORECASTDATE']).reset_index()

    return dfupdate, scaler


def transform_x_forecast(df, scaler):
    df_cat = df.loc[:, category_features]
    df_num = df.loc[:, numeric_features]

    cat_encoder = OneHotEncoder(sparse=False, categories='auto')
    cat_encoder.fit(df_cat)
    array_category = cat_encoder.transform(df_cat)
    category_name = cat_encoder.categories_
    columns_name = [x for item in category_name for x in item]

    scaler_new = StandardScaler()
    x = scaler_new.fit_transform(df_num.values)

    x_lr = scaler.transform(df_num.values)

    columns_name += numeric_features
    x_combine = np.concatenate([array_category, x, np.array(df['REVENUE']).reshape([-1, 1]), np.array(df['LG_REVENUE']).reshape([-1, 1])], axis=1)
    dfupdate = pd.DataFrame(x_combine, index=df['USER_ID'], columns=columns_name + ['REVENUE', 'LG_REVENUE']).reset_index()

    x_combine_lr = np.concatenate([array_category, x_lr, np.array(df['REVENUE']).reshape([-1, 1]), np.array(df['LG_REVENUE']).reshape([-1, 1])], axis=1)
    dfupdate_lr = pd.DataFrame(x_combine_lr, index=df['USER_ID'], columns=columns_name + ['REVENUE', 'LG_REVENUE']).reset_index()

    return dfupdate, dfupdate_lr

