import numpy as np
import pandas as pd
import lifetimes
import missingno as msno
import random
from global_variable import *
import matplotlib.pyplot as plt
import davesci as ds

con_write = ds.snowflake_connect(warehouse='DAVE_WH', role='DAVE_DATA_DEV')

def read_data():
    predict_df = pd.read_csv(datafile + predict_file, header=0)
    actual_df = pd.read_csv(datafile + actual_file)
    all_user = pd.read_csv(datafile + "users_property.csv")
    return predict_df, actual_df, all_user

def analyze_data(predict_df, actual_df):
    predict_update = predict_df.loc[:, ['userid', 'pred_num', 't_value', 'frequency', 'prob_alive']]
    actual_update = actual_df.loc[:, ['userid', 'trans_num', 'real_revenue']]
    df_compare = pd.merge(predict_update, actual_update, on=['userid'], how='left')
    df_compare = df_compare.fillna(0)

    involved_member = df_compare.shape[0]
    predict_month = predict_t / 30
    predict_revenue = predict_update['t_value'].sum()
    actual_revenue = actual_update['real_revenue'].sum()
    actual_churn = df_compare.loc[df_compare['real_revenue'] == 0, :].shape[0]

    predict_average_month = predict_revenue / (predict_month * involved_member)
    actual_average_month = actual_revenue / (predict_month * involved_member)
    predict_six_month = predict_revenue / involved_member
    actual_six_month = actual_revenue / involved_member
    predict_churn_rate = 1 - predict_update['prob_alive'].mean()
    actual_churn_rate = 1.0 * actual_churn / involved_member

    print("The total users considered here are " + str(involved_member))
    print("The predicted months are " + str(predict_month))
    print("The predicted revenue of these users is " + str(predict_six_month))
    print("The actual revenue of these users is " + str(actual_six_month))
    print("The predict churn rate of these users is " + str(predict_churn_rate))
    print("The actual churn rate is " + str(actual_churn_rate))

def generate_predict_result(predict_df, actual_df, all_user):
    df_user = all_user.loc[:, ['userid', 'startdate', 'platform', 'attribution', 'network']]
    predict_update = predict_df.loc[:, ['userid', 'first_trans','pred_num', 't_value', 'frequency', 'prob_alive', 'predict_clv']]
    actual_update = actual_df.loc[:, ['userid', 'trans_num', 'real_revenue']]

    combine_1 = pd.merge(df_user, predict_update, on=['userid'], how='left')
    combine_2 = pd.merge(combine_1, actual_update, on=['userid'], how='left')
    combine_2 = combine_2.fillna(0)
    combine_2['start_date'] = pd.to_datetime(combine_2['startdate'])
    combine_2['start_month'] = combine_2['start_date'].apply(lambda x: x.strftime('%Y-%m-01'))
    combine_2['predict_label'] = combine_2['first_trans'].apply(lambda x: 1 if x !=0 else 0)
    combine_2['actual_label'] = combine_2['trans_num'].apply(lambda x: 1 if x != 0 else 0)
    combine_2['tmp_label'] = combine_2['predict_label'] + combine_2['actual_label']
    combine_2['active_label'] = combine_2['tmp_label'].apply(lambda x: 1 if x !=0 else 0)
    combine_2 = combine_2.drop(['tmp_label', 'startdate'], axis=1)

    ds.write_snowflake_table(
        combine_2,
        "ANALYTIC_DB.MODEL_OUTPUT.statistical_training_result",
        con_write,
        mode="create",
        )


def main():
    predict_df, actual_df, all_user = read_data()
    #analyze_data(predict_df, actual_df)
    generate_predict_result(predict_df, actual_df, all_user)

if __name__ == '__main__':
    main()