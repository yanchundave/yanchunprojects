import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
import pickle
import math
from global_variable import *


"""
Step 2:

This is data clean and scale code snippet. It is before training part.

The input data are daily spending data for each channel and new daily PV users.

Remember to update global_variables and also response_file
"""
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


def channel_combine(df):
    '''
    Return a cleaned spending dataframe.

           Parameters:
                   df (dataframe): dataframe of daily spending. columns are "DATE, SPEND, NETWORK"

           Returns:
                   dfupdate (dataframe): dataframe of daily spennding.
                   columns are "Date, channels (six), spending, day, week, month, quarter
    '''

    df = stack_columns(df, minor_channels, 'minor_channels')
    df = stack_columns(df, new_channels, 'new_channels')
    df = df.drop(removed_channels, axis=1)
    df = df.drop(new_channels, axis=1)
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

    return dfupdate, spending_channels


def normalize_y(pv):
    return np.log(pv / y_constant + 1)


def combine_x_y(spending_pv, df_pv):
    '''
    Returns dataframe combine spending with pv after MinMaxScaler.

            Parameters:
                    spending_pv (dataframe): spending directly from csv file
                    df_pv(dataframe): new PV users from csv file
                    media_list (list): media to keep in analysis process
            Returns:
                    spending_scaler (np.array): spending of each channel after scaling
                    basic_scaler (np.array): basic value after scaling
                    y (array): new users
                    spending_tmp (np.array): spending without scaling
    '''

    scaler = MinMaxScaler()
    df_spending, spending_column = channel_combine(spending_pv)

    df_pv['y'] = normalize_y(df_pv['PV'])    # scale new users by actual new user divided by 1000 and get its log.
    df_combine = pd.merge(df_spending, df_pv, on=['date'], how='inner')
    df_combine = df_combine.sort_values(by=['datenumber'], ascending=True)
    df_combine['trend'] = np.log(df_combine.index + 1)
    df_combine['trend_update'] = scaler.fit_transform(np.array(df_combine['trend']).reshape(-1,1))

    seasonality_list = ['day', 'week', 'month', 'quarter']
    seasonality_constant = [31, 7, 12, 4]
    basic_columns = ['trend_update']
    for i, item in enumerate(seasonality_list):        # Add seasonality columns
        df_combine[item + '_sin'] = np.sin(df_combine[item] * 2 * math.pi / seasonality_constant[i])
        df_combine[item + '_cos'] = np.cos(df_combine[item] * 2 * math.pi / seasonality_constant[i])
        basic_columns.append(item + '_sin')
        basic_columns.append(item + '_cos')
    # Remove day_sin and day_cos
    basic_columns_update = ['trend_update', 'week_sin', 'week_cos', 'month_sin', 'month_cos', 'quarter_sin', 'quarter_cos']

    spending_tmp = df_combine.fillna(0).loc[:, spending_column].to_numpy()
    print(spending_column)

    scaler = MinMaxScaler()
    spending_scaler = scaler.fit_transform(spending_tmp)
    basic_scaler = df_combine.loc[:, basic_columns_update].to_numpy()

    return spending_scaler, basic_scaler, np.array(df_pv['y']), spending_tmp, spending_column


def pickle_dump(obj, datapath):
    with open(datapath, "wb") as f:
        pickle.dump(obj, f)


def obtain_limits(y):
    y_value = y.copy()
    return np.quantile(y_value, 0.025), np.quantile(y_value, 0.975)


def data_input():
    '''
    Input the spending and pv from csv.
    Clip the outliers and limit the y within 1.5 and 2.4 after investigation.
    Dump the scaled spending, pv, origin_spending to pickle files for model training and analysis.
    '''

    pv_daily = "platform_user_advance.csv" if FLAG == 1 else "total_revenue.csv"
    spending_daily = "channel_spending_raw.csv"
    df_pv = pd.read_csv(datafile_path + pv_daily)
    spending_pv = pd.read_csv(datafile_path + spending_daily)

    spending, basic, y, spending_origin, spending_column = combine_x_y(spending_pv, df_pv)
    lowerlimit, upper_limit = obtain_limits(y)
    y = np.clip(y, lowerlimit, upper_limit)

    pickle_dump(spending, datafile_path + "spending.p")
    pickle_dump(basic, datafile_path + "basic.p")
    pickle_dump(y, datafile_path + "newuser.p")
    pickle_dump(spending_origin, datafile_path + "spending_origin.p")
    # save spending column for training data
    with open("spending_column.txt", 'w') as f:
        for item in spending_column:
            f.write(item + ",")
    print("Done")


def main():
    data_input()


if __name__ == '__main__':
    main()
