import numpy as np
import pandas as pd
import math
import matplotlib.pyplot as plt
from sklearn import metrics
import statsmodels.api as sm
import pickle
from collections import defaultdict
import seaborn as sns
from datetime import datetime
from global_variable import *
from result_calculation import *
from result_graph import *
from result_others import *


"""
Step 4: This is result analysis code snippet. It is after traning part.

The input data are spending origin, scaled spending, scaled basic, scaled new users and model result.
"""


def read_data():
    '''
    Read scaled spending, basic, new users and spending origin
            Return:
                spending (np.array): scaled spending
                basic (np.array): scaled basic variable
                newuser (list): scaled new user
    '''

    with open(datafile_path + "spending.p", "rb") as f:
        spending = pickle.load(f)
    with open(datafile_path + "basic.p", "rb") as f:
        basic = pickle.load(f)
    with open(datafile_path + "newuser.p", "rb") as f:
        newuser = pickle.load(f)
    with open(datafile_path + "spending_origin.p", "rb") as f:
        spending_origin = pickle.load(f)

    return spending, basic, newuser, spending_origin


def read_parameters():
    '''
    Read model training result and spending origin.
    For keeping the flexibility of calling different function, duplicate the spending_origin reading.
            Return:
                    df_parameter (dataframe): model traning result
                    spending_origin (np.array): spending origin array
    '''

    with open(datafile_path + "pystan_latest_train.p", "rb") as f:
        df_parameter = pickle.load(f)

    return df_parameter


def main():

    spending, basic, newuser, spending_origin = read_data()
    df_parameter = read_parameters()

    with open("parameter.p", 'rb') as f:
        stan = pickle.load(f)

    Km = stan['Km']
    Kl = stan['Kl']
    Kb = stan['Kb']
    L = stan['L']
    T = stan['T']

    with open("spending_column.txt", 'r') as f:
        media_list = f.read().strip().split(",")[0:-1]

    # Check prediction effectiveness. Basically checking the R-square
    get_prediction_from_samples(df_parameter, newuser, Kl)

    # Plot parameter distribution, within result_graph.py
    plot_distribution(df_parameter, Km, Kb)

    # Draw Original Spending
    draw_origin_spending(spending_origin, Km, media_list)

    # Draw all data saturation
    results, spending_list, current_spending = actual_all_data_saturation(spending_origin, df_parameter, Km, L)
    draw_all_data_saturation(results, spending_list, current_spending, media_list, Km)
    draw_platform_data_saturation(results, spending_list, current_spending, media_list, Km)

    results, spending_list, current_spending = actual_data_saturation(spending_origin, df_parameter, Km, L)
    draw_actual_saturation(results, spending_list, current_spending, media_list, Km)

    draw_seaonality(df_parameter, basic, Kl)

    # Channel contribution
    total, contribution = calculate_prediction_and_prediction_removed(df_parameter, Km, Kl)
    draw_contribution(total, contribution, media_list)

    # Channel Information
    draw_channel_information(df_parameter, spending_origin, Km, Kl, T, media_list)

    # Decay Effect
    draw_decay_effect(df_parameter, Km, media_list)

    print("Done")


if __name__ == '__main__':
    main()


