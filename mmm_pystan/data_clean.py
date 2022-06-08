"""This is data clean and scale code snippet. It is before training part.

The input data are daily spending data for each channel and new daily PV users.
"""


import numpy as np 
import pandas as pd 
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
import pickle 
import math

#flag = 1 # user
flag = 0 # revenue
#response_file = "platform_user.csv"
response_file = "total_revenue.csv"
#response_file = "platform_user_advance.csv"
independent_file = "channel_spending_raw.csv"


def stack_columns(df, column_list, column_name):
    df[column_name] = df[column_list[0]]
    for item in column_list[1:]:
        df[column_name] = df[column_name] + df[item]
    return df

def clean_spending_update(df):
    '''
    Return a cleaned spending dataframe.

           Parameters:
                   df (dataframe): dataframe of daily spending. columns are "DATE, SPEND, NETWORK"

           Returns:
                   dfupdate (dataframe): dataframe of daily spennding. 
                   columns are "Date, channels (six), spending, day, week, month, quarter
    '''

    column_name = [x for x in df.columns if ('BRANDING' not in x) and ('Taboola' not in x)]
    unknown_column = [x for x in column_name if ('unknown' in x) and ('Videoamp' not in x)]
    tv_column = [x for x in column_name if ('Tatari' in x) or ('Videoamp' in x)]
    df = stack_columns(df, unknown_column, 'unknown')
    df = stack_columns(df, tv_column, 'TV')

    column_name_update = [x for x in column_name if (x not in unknown_column) and (x not in tv_column) and ('Unnamed' not in x)]
    column_name_update += ['unknown', 'TV']

    df['date_update'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')
    df['datenumber'] = df['date_update'].dt.strftime('%Y-%m-%d')
    df['day'] = df['date_update'].dt.day
    df['week'] = df['date_update'].dt.dayofweek
    df['month'] = df['date_update'].dt.month
    df['quarter'] = df['date_update'].dt.quarter

    column_name_update += ['datenumber', 'day', 'week', 'month', 'quarter']
    dfupdate = df.loc[:, column_name_update]
    return dfupdate

def normalize_y(pv, flag):
    if flag == 1:
        return np.log(pv / 1000 + 1)
    else:
        return np.log(pv / 100000)

def combine_x_y(spending_pv, df_pv, flag):
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
    df_spending = clean_spending_update(spending_pv)
    spending_column = [x for x in df_spending.columns if ('Android' in x) or ('iOS' in x)] + ['unknown', 'TV']
    df_pv['y'] = normalize_y(df_pv['PV'], flag)    # scale new users by actual new user divided by 1000 and get its log.
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
   
    basic_columns_update = ['trend_update', 'week_sin', 'week_cos', 'month_sin', 'month_cos', 'quarter_sin', 'quarter_cos']

    spending_tmp = df_combine.fillna(0).loc[:, spending_column].to_numpy()
    print(spending_column)

    scaler = MinMaxScaler()
    spending_scaler = scaler.fit_transform(spending_tmp)
    basic_scaler = df_combine.loc[:, basic_columns_update].to_numpy()
    return spending_scaler, basic_scaler, np.array(df_pv['y']), spending_tmp


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

    datafile_path = "/Users/yanchunyang/Documents/datafiles/"
    pv_daily = response_file
    spending_daily = independent_file
    df_pv = pd.read_csv(datafile_path + pv_daily)
    spending_pv = pd.read_csv(datafile_path + spending_daily)
    spending, basic, y, spending_origin = combine_x_y(spending_pv, df_pv, flag)
    lowerlimit, upper_limit = obtain_limits(y)
    y = np.clip(y, lowerlimit, upper_limit)
    pickle_dump(spending, datafile_path + "pystan/spending.p")
    pickle_dump(basic, datafile_path + "pystan/basic.p")
    pickle_dump(y, datafile_path + "pystan/newuser.p")
    pickle_dump(spending_origin, datafile_path + "pystan/spending_origin.p")
    print("Done")

def main():
    data_input()


if __name__ == '__main__':
    main()
    