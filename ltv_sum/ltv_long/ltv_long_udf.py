import numpy as np
import pandas as pd
import random
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from ltv_long_global import *
import math

def category_clean(df):
    for item in category_features:
        df[item] = df[item].astype(str)
        df[item].fillna(item+'_None', inplace=True)
        df.loc[df[item]=='None', [item]] = item + '_None'
        df[item] = df[item].str.upper()
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

# to get recency std
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

def feature_derived(df):
    # update y
    df['LG_REVENUE'] = df['REALREVENUE'].apply(lambda x: 1 if x > 0 else -1)
    df.loc[~df['NETWORK'].isin(channels), ['NETWORK']] = 'NETWORK_OTHERS'
    #network organice, referral comflicts with attribution's organic and referral
    df['NETWORK'] = df['NETWORK'].apply(lambda x: x + '_NETWORK')
    df['MONTH'] = df['STARTDATE'].apply(lambda x: int(str(x)[5:7]))
    df['SINE_MONTH'] = df['MONTH'].apply(lambda x: math.sin(2 * math.pi * x / 12))
    df['COSINE_MONTH'] = df['MONTH'].apply(lambda x: math.cos(2 * math.pi * x / 12))
    df['TIMEDIFF_STD'] = df['TRANS_LIST'].apply(lambda x: calculate_std_derived(x))
    df['MONETARY_STD'] = df['MONETARY_LIST'].apply(lambda x: calculate_std(x))
    df['SESSION_STD'] = df['SESSION_LIST'].apply(lambda x: calculate_std(x))
    df['ACTIVEMONTH'] = df['SESSION_LIST'].apply(lambda x: calculate_listlength(x))

    df = category_clean(df)
    df = df.fillna(0)
    return df

def transform_x(df):
    df_cat = df.loc[:, category_features]
    df_num = df.loc[:, numeric_features]
    imputer = SimpleImputer(strategy='median')
    cat_encoder = OneHotEncoder(sparse=False, categories='auto')
    cat_encoder.fit(df_cat)
    array_category = cat_encoder.transform(df_cat)
    category_name = cat_encoder.categories_
    columns_name = [x for item in category_name for x in item]

    scaler = StandardScaler()
    x = imputer.fit_transform(df_num)
    x = scaler.fit_transform(x)

    columns_name += numeric_features
    x_combine = np.concatenate([array_category, x, np.array(df['REALREVENUE']).reshape([-1, 1]), np.array(df['LG_REVENUE']).reshape([-1, 1])], axis=1)
    dfupdate = pd.DataFrame(x_combine, index=df['USER_ID'], columns=columns_name + ['REALREVENUE', 'LG_REVENUE']).reset_index()

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
    x_combine = np.concatenate([array_category, x, np.array(df['REALREVENUE']).reshape([-1, 1]), np.array(df['LG_REVENUE']).reshape([-1, 1])], axis=1)
    dfupdate = pd.DataFrame(x_combine, index=df['USER_ID'], columns=columns_name + ['REALREVENUE', 'LG_REVENUE']).reset_index()

    x_combine_lr = np.concatenate([array_category, x_lr, np.array(df['REALREVENUE']).reshape([-1, 1]), np.array(df['LG_REVENUE']).reshape([-1, 1])], axis=1)
    dfupdate_lr = pd.DataFrame(x_combine_lr, index=df['USER_ID'], columns=columns_name + ['REALREVENUE', 'LG_REVENUE']).reset_index()

    return dfupdate, dfupdate_lr


def split_train_test(df, ratio):
    shuffle_index = np.random.permutation(len(df))
    test_size = int(len(df) * ratio)
    test_index = shuffle_index[:test_size]
    train_index = shuffle_index[test_size:]
    return df.iloc[train_index], df.iloc[test_index]