import numpy as np
import pandas as pd 

datafile = "/Users/yanchunyang/Documents/datafiles/"
spending_file = "platform_raw.csv"
#user_file = "total_revenue.csv"
user_file = "platform_user_advance.csv"

def generate_date_for_robyn():
    spending = pd.read_csv(datafile + spending_file, header=0)
    dependent = pd.read_csv(datafile + user_file)
    df_user = dependent.loc[:, ['date', 'PV']]
    df_spending = spending.loc[:, ['date', 'channel', 'platform', 'spending']]
    df_spending['channel_spending'] = df_spending['channel'].astype(str) + "_" + df_spending['platform'].astype(str)
    df_spending_pivot = pd.pivot_table(df_spending, values='spending', index=['date'], columns=['channel_spending'], 
        aggfunc=np.sum, fill_value = 0)
    columns = df_spending_pivot.columns
    unknown_list = [x for x in columns if 'unknown' in x]
    df_spending_pivot['unknown'] = df_spending_pivot[unknown_list[0]]
    for item in unknown_list[1:]:
        df_spending_pivot['unknown'] += df_spending_pivot[item]
    df_spending_pivot = df_spending_pivot.drop(unknown_list, axis = 1)
    df_data = pd.merge(df_spending_pivot, df_user, on=['date'], how='inner')
    df_data_update = df_data.drop(['Taboola_Android', 'Taboola_iOS'], axis=1)
    df_data_update.to_csv(datafile + "mmm_r_raw_one.csv")

def pseudo_data():
    df = pd.read_csv(datafile + "mmm_r_raw_one.csv", header=0)
    print(df.columns)
    indexes = [ "index", 'date', 'AA', 'AI', 'AS', 'LA', 'LI', 'RA', 'RI', 'SA', 'SI', 'TT', 'BA', 'BI', 'UN', 'PV']
    df.columns = indexes
    # encrypt begin
    spending_columns = ['AA', 'AI', 'AS', 'LA', 'LI', 'RA', 'RI', 'SA', 'SI', 'TT', 'BA', 'BI', 'UN']
    for col in spending_columns:
        df[col] = df[col] / 10.0
    # encrypt end
    df.to_csv(datafile + "mmm_r_advance.csv")

def main():
    #generate_date_for_robyn()
    pseudo_data()

if __name__ == '__main__':
    main()