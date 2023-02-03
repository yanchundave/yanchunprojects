import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
import pickle
import math


minor_channels = [
    'bytedanceglobal_int_unknown',
    'Taboola_iOS',
    'Adwords_unknown',
    'Reddit_iOS',
    'Taboola_Android',
    'Reddit_Android',
    'Applovin_Android',
    'Facebook_unknown',
    'Snapchat_unknown',
    ]

new_channels = [
    'YouTube_unknown',
    'Videoamp_unknown',
    'Streaming_unknown',
    'National_Radio_unknown',
    'Podcast_unknown',
    'Local_Radio_unknown'
    ]

removed_channels = ['BRANDING_unknown']

major_channels = [
    'Snapchat_Android',
    'bytedanceglobal_int_Android',
    'bytedanceglobal_int_iOS',
    'Snapchat_iOS',
    'Adwords_iOS',
    'Apple_Search_Ads_iOS',
    'Tatari_TV',
    'Facebook_Android',
    'Facebook_iOS',
    'Adwords_Android'
    ]


def stack_columns(df, column_list, column_name):
    df[column_name] = df[column_list[0]]
    for item in column_list[1:]:
        df[column_name] = df[column_name] + df[item]
    return df

def update_channel(columnname):
    minor_list = []
    new_list = []
    for item in columnname:
        if item in major_channels:
            continue
        elif item in minor_channels:
            minor_list.append(item)
        else:
            new_list.append(item)
    return minor_list, new_list

def channel_combine(df):
    '''
    Return a cleaned spending dataframe.

           Parameters:
                   df (dataframe): dataframe of daily spending. columns are "DATE, SPEND, NETWORK"

           Returns:
                   dfupdate (dataframe): dataframe of daily spennding.
                   columns are "Date, channels (six), spending, day, week, month, quarter
    '''
    minor_channel_update, new_channel_update = update_channel(df.columns[2:])
    df = stack_columns(df, minor_channel_update, 'minor_channels')
    df = stack_columns(df, new_channel_update, 'new_channels')
    for item in removed_channels:
        if item in df.columns:
            df = df.drop(removed_channels, axis=1)
    df = df.drop(new_channels, axis=1)
    for itme in minor_channels:
        if item in df.columns:
            df = df.drop(minor_channels, axis=1)

    df['date_update'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')
    df['datenumber'] = df['date_update'].dt.strftime('%Y-%m-%d')
    df['day'] = df['date_update'].dt.day
    df['week'] = df['date_update'].dt.dayofweek
    df['month'] = df['date_update'].dt.month
    df['quarter'] = df['date_update'].dt.quarter

    column_name_update = major_channels + ['date', 'datenumber', 'day', 'week', 'month', 'quarter', 'minor_channels', 'new_channels']
    dfupdate = df.loc[:, column_name_update]
    spending_channels = major_channels + ['minor_channels', 'new_channels']

    # Remove minor_channels
    #dfupdate_1 = dfupdate.drop(['minor_channels'], axis=1)
    #spending_channels.remove("minor_channels")

    return dfupdate, spending_channels