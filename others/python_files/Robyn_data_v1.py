import numpy as np
import pandas as pd
from global_variable import *

spending_file = "platform_raw.csv"
user_file = response_file
channel_platform = False

def generate_date_for_robyn():
    spending = pd.read_csv(datafile_path + spending_file, header=0)
    dependent = pd.read_csv(datafile_path + user_file)
    df_user = dependent.loc[:, ['date', 'PV']]
    df_spending = spending.loc[:, ['date', 'channel', 'platform', 'spending']]
    if channel_platform:
    # if channel has be considered by channel + platform
        df_spending['channel_spending'] = df_spending['channel'].astype(str) + "_" + df_spending['platform'].astype(str)
    else:
    # if channel don't split between android and ios
        df_spending['channel_spending'] = df_spending['channel'].astype(str)
    df_spending_pivot = pd.pivot_table(df_spending, values='spending', index=['date'], columns=['channel_spending'],
        aggfunc=np.sum, fill_value = 0)
    columns = list(df_spending_pivot.columns)
    if channel_platform:
        unknown_list = [x for x in columns if 'unknown' in x]
        df_spending_pivot['unknown'] = df_spending_pivot[unknown_list[0]]
        for item in unknown_list[1:]:
            df_spending_pivot['unknown'] += df_spending_pivot[item]
        df_spending_pivot = df_spending_pivot.drop(unknown_list, axis = 1)
        print(columns)
        df_spending_pivot.columns = [x for x in columns if 'unknown' not in x] + ['unknown']
    else:
        df_spending_pivot.columns = [x for x in columns if 'unknown' not in x]
    #df_spending_pivot.columns = []
    df_data = pd.merge(df_spending_pivot, df_user, on=['date'], how='inner')
    if channel_platform:
        df_data_update = df_data.drop(['Taboola_Android', 'Taboola_iOS'], axis=1)
    else:
        df_data_update = df_data.drop(['Applovin', 'BRANDING'], axis=1)
    df_columns = df_data_update.columns
    df_columns_update = [x.replace(" ", "_") for x in df_columns]
    df_data_update.columns = df_columns_update
    """
    Induce the macro factors
    """
    macro_df = pd.read_csv("/Users/yanchunyang/Documents/dave_projects/scripts/mmm_pystan/" + "macro_information.csv")
    macro_update = macro_df.loc[:, ['datenumber', 'inflation', 'chime', 'varo', 'mobilebank']]
    macro_update['date'] = macro_update['datenumber']
    macro_update.drop(['datenumber'], axis=1)
    df_data_update_1 = pd.merge(df_data_update, macro_update, on=['date'])
    """
    End of macro factors
    """
    df_data_update_1.to_csv(datafile_path + "mmm_r_raw_one.csv")
    print(df_data_update.columns)

def pseudo_data():
    df = pd.read_csv(datafile_path + "mmm_r_raw_one.csv", header=0)
    print(df.columns)
    # encrypt begin
    spending_columns = ['Adwords_Android', 'Adwords_iOS',
       'Apple_Search_Ads_iOS', 'Facebook_Android', 'Facebook_iOS',
        'Snapchat_Android', 'Snapchat_iOS',
        'bytedanceglobal_int_Android', 'bytedanceglobal_int_iOS',
       'unknown']
    for col in spending_columns:
        df[col] = df[col] / 10.0
    # encrypt end
    df.to_csv(datafile_path + "mmm_r_advance.csv")

def main():
    print(datafile_path)
    generate_date_for_robyn()
    #pseudo_data()

if __name__ == '__main__':
    main()