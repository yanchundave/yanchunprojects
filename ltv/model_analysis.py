import numpy as np 
import pandas as pd 
import lifetimes 
import missingno as msno
import random
from global_variable import *
import matplotlib.pyplot as plt 


def read_data():
    predict_df = pd.read_csv(datafile + predict_file, header=0)
    actual_df = pd.read_csv(datafile + actual_file)
    return predict_df, actual_df

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
    
def main():
    predict_df, actual_df = read_data()
    analyze_data(predict_df, actual_df)

if __name__ == '__main__':
    main()