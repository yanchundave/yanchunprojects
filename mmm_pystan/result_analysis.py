"""This is result analysis code snippet. It is after traning part.

The input data are spending origin, scaled spending, scaled basic, scaled new users and model result.
"""


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

            
def main(argv): 

    spending, basic, newuser, spending_origin = read_data()
    df_parameter = read_parameters()

    if argv == 'get_prediction_from_samples':
        get_prediction_from_samples(df_parameter, spending, basic, newuser, spending_origin)

    elif argv == 'plot_distribution':
        plot_distribution(df_parameter)

    elif argv == 'draw_origin_spending':
        draw_origin_spending(spending_origin)

    elif argv == 'actual_all_data_saturation':  # draw saturation graph bachsed on the actual spending
        results, spending_list, current_spending = actual_all_data_saturation(spending_origin, df_parameter)
        draw_all_data_saturation(results, spending_list, current_spending)

    elif argv == 'platform_data_saturation':  # draw saturation graph bachsed on the actual spending
        results, spending_list, current_spending = actual_all_data_saturation(spending_origin, df_parameter)
        draw_platform_data_saturation(results, spending_list, current_spending)

    elif argv == 'actual_data_saturation': # saturation graph for each platform
        results, spending_list, current_spending = actual_data_saturation(spending_origin, df_parameter)
        draw_actual_saturation(results, spending_list, current_spending)

    elif argv == 'draw_seasonality':     # draw seasonality
        draw_seaonality(df_parameter, basic)

    elif 'channel_contribution' in argv:
        splits = argv.strip().split(" ")
        total, contribution = calculate_prediction_and_prediction_removed(df_parameter, splits[-1])
        draw_contribution(total, contribution)
       
    elif argv == 'channel_information':
        draw_channel_information(df_parameter, spending_origin)
    
    elif argv == 'decay_effect':
        draw_decay_effect(df_parameter)

    else:
        print("The function you input is not in the function list")

if __name__ == '__main__':
    fun_name = input("Please input the function you want to run: \n")
    main(fun_name)


