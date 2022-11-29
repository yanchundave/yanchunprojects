import numpy as np
import pandas as pd
from global_variable import *
"""
Inflation rate is coming from https://www.rateinflation.com/inflation-rate/usa-inflation-rate/

competiton work is coming from https://trends.google.com/trends/explore?q=chime&geo=US
"""

spending_file = "platform_raw.csv"
user_file ="platform_user_advance.csv" if FLAG == 1 else "total_revenue.csv"
channel_platform = True

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
    df[column_name] = 0
    for item in column_list:
        if item in df.columns:
            df[column_name] = df[column_name] + df[item]
    return df

def drop_columns(df, drop_list):
    for item in drop_list:
        if item in df.columns:
            df.drop([item], axis=1, inplace=True)
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

    df = drop_columns(df, removed_channels)
    df = drop_columns(df, new_channels)
    df = drop_columns(df, minor_channels)

    column_name_update = major_channels + ['date', 'minor_channels', 'new_channels']
    dfupdate = df.loc[:, column_name_update]
    spending_channels = major_channels + ['minor_channels', 'new_channels']

    return dfupdate, spending_channels

def get_macro_data():
    inflation_file = "inflation_rate.txt"
    competition_file = "multiTimeline.csv"
    with open(inflation_file, 'r') as f:
        line = f.readline()
        splits = line.strip().split()
        print(splits)
        splits_update = [float(x[:-1]) for x in splits]
    start_month = "2021-01-01"
    end_month = "2022-11-01"
    months = pd.date_range(start=start_month, end=end_month, freq='M')
    inflation = pd.DataFrame(splits_update, index=months).reset_index()
    inflation.columns = ['date', 'inflation']

    competition_df = pd.read_csv(competition_file, header=None)
    competition_df.columns = ['datestr', 'competition']
    competition_df['date'] = pd.to_datetime(competition_df['datestr'])
    date_col = pd.date_range(start='2021-01-01', end='2022-11-20', freq='D')
    date_dim = pd.DataFrame(np.array([1] * len(date_col)), index=date_col).reset_index()
    date_dim.columns = ['date', 'constantvalue']

    df_comp = pd.merge(date_dim, competition_df, on=['date'], how='left')
    df_comp_inflation = pd.merge(df_comp, inflation, on=['date'], how='left')
    df_comp_inflation = df_comp_inflation.fillna(method = 'ffill')
    df_comp_inflation = df_comp_inflation.fillna(method = 'bfill')
    df_comp_inflation = df_comp_inflation.drop(['datestr'], axis=1)
    df_comp_inflation['date'] = df_comp_inflation['date'].astype('str')

    return df_comp_inflation


def update_columnname(columnname):
    return [x.replace(" ", "_") for x in columnname]


def generate_date_for_robyn():

    spending = pd.read_csv(datafile_path + spending_file, header=0)
    dependent = pd.read_csv(datafile_path + user_file)

    df_user = dependent.loc[:, ['date', 'PV']]
    df_spending = spending.loc[:, ['date', 'channel', 'platform', 'spending']]

    df_spending['channel_spending'] = df_spending['channel'].astype(str) + "_" + df_spending['platform'].astype(str)

    df_spending_pivot = pd.pivot_table(df_spending, values='spending', index=['date'], columns=['channel_spending'],
        aggfunc=np.sum, fill_value = 0)

    df_spending_pivot.columns = update_columnname(df_spending_pivot.columns)

    columns = list(df_spending_pivot.columns)

    df_spending_update, spending_column = channel_combine(df_spending_pivot.reset_index())
    df_data = pd.merge(df_spending_update, df_user, on=['date'], how='inner')

    macro_update = get_macro_data()
    df_data_update = pd.merge(df_data, macro_update, on=['date'])

    df_data_update = df_data_update.drop(['constantvalue'], axis=1)

    # Drop minor channels
    #df_data_update = df_data_update.drop(['minor_channels'], axis=1)

    print(df_data_update.loc[:, ['date']].tail(10))

    df_data_update.to_csv(datafile_path + "mmm_r_raw_one.csv")
    return spending_column


def get_parameter(spending_column):

    #columns.remove("minor_channels")
    with open("robyn_parameters.txt", "w") as f:
        for item in spending_column:
            f.write(item + "_alphas=c(0.5, 4)," + "\n")
            f.write(item + "_gammas=c(0.3,1)," + "\n")
            f.write(item + "_shapes=c(0.0001,2)," + "\n")
            f.write(item + "_scales=c(0, 0.1)," + "\n")
            f.write("\n")



def main():
    print(datafile_path)
    spending_column = generate_date_for_robyn()
    get_parameter(spending_column)

if __name__ == '__main__':
    main()