import numpy as np
import pandas as pd
from global_variable import *
import math


"""
This file is to support result_analysis.py.
It provides the necessary calculation functions to draw the graph
"""

"""
The below functions are for the first analysis function to get_prediction_from_samples
"""

def moving_average(a, n):  #For keeping this script independently and conviniently, duplicate this function
    '''
    Return the moving average of a list based on the moving number.

            Parameters:
                    a (list): the list for moving average
                    n (integer): the number for moving avearge
    '''
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n-1:] / n


def y_fitted_from_bayesian(df_parameter, Kl):
    '''
    Returns the prediction of y from the posterior distribution
            Parameters:
                df_parameter (dataframe): training model result
            Returns:
                y_values (np.array): mean of prediction of y from posterior distribution
    '''

    y_columns = ["y_fitted." + str(i) for i in range(1, Kl + 1)]
    y_values = df_parameter.loc[:, y_columns].values
    return np.mean(y_values, axis=0)


def get_prediction_from_samples(df_parameter, newuser,  Kl):
    '''
    Calculated R-squared of prediciton and the actual value
            Parameters:
                    df_parameter (dataframe): training model result
            Returns:
                    None, print R-squared of prediction and actual value
                    Print R-squared of part of list with part of actual value (To test train and test dataset)
    '''
    y = moving_average(newuser[:-1], 7)
    y_from_model = y_fitted_from_bayesian(df_parameter, Kl)
    print("y directly from model")
    r_square = np.square(np.corrcoef(y_from_model, y))
    #print(np.square(np.corrcoef(y_from_model[0:kl], y[0:kl])))
    with open("model_result.txt", 'w') as f:
        f.write("Get y prediction for the model" + "\n")
        f.write(str(r_square) + "\n")


"""
Calculate ROAS
"""

def get_roas(y_origin, remove_list, spending_list, Kl, T):
    '''
    Returns the channels' weekly average ROAS
            Parameters:
                    y_origin: the y value without removing any channel. y_origin is the value after scaling.
                    remove_list: the prediction of y value ater removing the special channel
                    spending_list: the specific channel's spending
            Return:
                    df_weekly_roas (dataframe): the weekly average roas for specific channel
    '''

    spending_update = spending_list[T-1: -1]
    newuser_origin = np.exp(y_origin) * y_constant
    newuser_remove = np.exp(remove_list) * y_constant
    y_diff = newuser_origin - newuser_remove
    day_index = np.arange(1, Kl+1)
    datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(ORIGIN_DATE)) #After moving average, the data is from Jan 8
    df = pd.DataFrame(y_diff, columns = ['newuser'])
    df['datetime'] = datetimes
    df['spending'] = spending_update
    df['weekly'] = df['datetime'].dt.strftime('%Y-%U')
    df_weekly_roas = df.groupby(['weekly']).agg({'spending':'sum', 'newuser':'sum', 'datetime':'min'}).reset_index()
    y_value = np.array([max(x, 0) for x in df_weekly_roas['newuser']])
    df_weekly_roas['weekly_roas'] = y_value/ df_weekly_roas['spending']
    return df_weekly_roas


def get_roas_accumulate(y_origin, remove_list, spending_list, T):
    '''
    Return the channel's roas trending from beginning to end
            Parameters:
                    y_origin: the y value without removing any channel. y_origin is the value after scaling.
                    remove_list: the prediction of y value ater removing the special channel
                    spending_list: the specific channel's spending
            Return:
                    roas (np.array): the specific channel's accumulative roas list
    '''

    spending_update = spending_list[T-1: -1]
    newuser_origin = np.exp(y_origin) * y_constant
    newuser_remove = np.exp(remove_list) * y_constant
    y_diff = newuser_origin - newuser_remove
    y_cum = np.cumsum(y_diff)
    spending_cum = np.cumsum(spending_update)
    roas = y_cum / spending_cum
    roas = y_diff / (spending_update + 0.0001)  # in case 0 spending_update
    return roas


"""
Calculate the dropping rate for each channel
"""

def calcualte_droping_rate(origin, remove):
    '''
    Return total dropping rate after remove some specific channel
            Parameters:
                    origin (np.array): the new user value list without moving any channel
                    remove (np.array): the new user value list after moving some channel
            Return:
                    dropping rate (float): the overall dropping rate of specific channel
    '''
    diff = origin - remove
    diff_update = [x for x in diff if x >= 0 and x < np.inf]
    return sum(diff_update) / sum([x for x in origin if x>=0 and x < np.inf])


"""
Calculate Saturation
"""

def get_coes(df_parameter, Km, L):
    '''
    Return coes for each channel. Assume the daily spending is same, its actual spending's effect should be multipled by a coefficient.
            Parameters:
                    df_parameter (dataframe): Training model result
            Return:
                    coes for each channel
    '''

    alpha_cols = ["alpha." + str(i) for i in range(1, Km + 1)]
    #theta_cols = ["theta." + str(i) for i in range(1, Km + 1)]
    alpha_df = df_parameter.loc[:, alpha_cols].values
    #theta_df = df_parameter.loc[:, theta_cols].values
    alpha = np.mean(alpha_df, axis=0)
    #theta = np.mean(theta_df, axis=0)
    coes = []
    for i in range(0, Km):
        #tmp = [math.pow(alpha[i], (t - theta[i])**2) for t in range(13, -1, -1)]
        tmp = [math.pow(alpha[i], t) for t in range(L-1, -1, -1)]
        coes.append(sum(tmp))
    return coes


def actual_all_data_saturation(spending_origin, df_parameter, Km, L):
    '''
    This function is to get the data for draw saturation curve with actual spending labelled on the graph.
    Since the carryover effect exists, the actual spending's effect need multiple an effect coefficient.
            Parameters:
                    spending_origin (np.array): the actual spending on each channel
                    df_parameter (dataframe): the training model result
            Return:
                    results (list of effect): the spending effect with carryover effect
                    spending_list (list of spending): spending value on x axle.
                    current_spending: the actual spending
    '''

    beta_m_columns = ['beta_m.' + str(i) for i in range(1, Km + 1)]
    beta_m_values = df_parameter.loc[:, beta_m_columns].values
    beta_m = np.mean(beta_m_values, axis=0)

    minvalue_each_network = np.min(spending_origin, axis=0)
    maxvalue_each_network = np.max(spending_origin, axis=0)
    meanvalue_each_network = np.mean(spending_origin, axis=0)

    coes = get_coes(df_parameter, Km, L)

    maxtotal = np.max(maxvalue_each_network)
    mintotal = np.min(minvalue_each_network)
    spending_diff = (maxtotal * 2 - mintotal) / 1000
    spending_list = np.array([mintotal + t * spending_diff for t in range(0, 1000)])

    results = [] #y_value
    current_spending = []

    for i in range(0, Km):
        spending_scaler = (spending_list - minvalue_each_network[i]) / (maxvalue_each_network[i] - minvalue_each_network[i])
        spending_y = np.log(spending_scaler * coes[i] + 1)
        spending_update = beta_m[i] * spending_y
        spending_update[spending_update < 0] = None
        results.append(spending_update)
        tmp = (meanvalue_each_network[i] - meanvalue_each_network[i]) / (maxvalue_each_network[i] - minvalue_each_network[i])
        tmp_value = beta_m[i] * math.log(tmp * coes[i] + 1)
        current_spending.append([meanvalue_each_network[i], tmp_value])
    return results, spending_list, current_spending


def actual_data_saturation(spending_origin, df_parameter, Km, L):
    '''
    Prepare the dataset for saturation curves.
            Parameters:
                    spending_origin (np.array): the actual spending without scaling
                    df_parameter (dataframe): training model result
            Return:
                    results (list): the actual spending effect
                    spending_total (list): the spending value for x axis
                    current_spending: the actual spending
    '''

    beta_m_columns = ['beta_m.' + str(i) for i in range(1, Km + 1)]
    beta_m_values = df_parameter.loc[:, beta_m_columns].values
    beta_m = np.mean(beta_m_values, axis=0)

    coes = get_coes(df_parameter, Km, L)
    minvalue_each_network = np.min(spending_origin, axis=0)
    maxvalue_each_network = np.max(spending_origin, axis=0)

    results = [] #y_value
    current_spending = []
    spending_total = []
    for i in range(0, Km):
        mintotal = minvalue_each_network[i]
        maxtotal = maxvalue_each_network[i]
        spending_diff = (maxtotal * 2 - mintotal) / 1000
        spending_list = np.array([mintotal + t * spending_diff for t in range(0, 1000)])
        spending_total.append(spending_list)

        spending_scaler = (spending_list - mintotal) / (maxtotal - mintotal)
        spending_y = np.log(spending_scaler * coes[i] + 1)
        results.append(beta_m[i] * spending_y)

        tmp = np.sort(spending_origin[:, i])
        network_current = [np.mean(tmp), np.percentile(tmp, 2.5), np.percentile(tmp, 97.5)]
        current_spending.append(network_current)

    return results, spending_total, current_spending


"""
Each channel's contribution saved in csv

The below is the analysis to calculate the difference between all the channels' prediction and prediction after removing special channel
The difference shows the contribution from some channel.
I will update the model dependent variable to calculate the contribution for converted users
The ratio of two experiments will show each channels conversion rate

"""
def calculate_prediction_and_prediction_removed(df_parameter, Km, Kl):
    '''
    This function is to read data from test result.
    We can obtain the prediction of all channels and also prediction after removing some channel
    The difference is the contribution of some channel and save to hardware for later comparision.
    '''
    y_from_model = y_fitted_from_bayesian(df_parameter, Kl)
    remove_result = []
    for i in range(1, Km+1):
        roas_columns = ["y_remove." + str(i) + "." + str(j) for j in range(1, Kl + 1)]
        y_values = df_parameter.loc[:, roas_columns].values
        remove_result.append(np.mean(y_values, axis=0))

    y_total = y_from_model.reshape((-1,1))
    y_remove = np.array(remove_result).T
    newuser_origin = np.exp(y_total) * y_constant
    newuser_remove = np.exp(y_remove) * y_constant
    y_diff = newuser_origin - newuser_remove

    """
    result_df = pd.DataFrame(y_diff, columns = media_list)
    result_df.to_csv(datafile_path + label + ".csv")

    newuser_origin_df = pd.DataFrame(newuser_origin)
    newuser_origin_df.to_csv(datafile_path + "newuser_origin.csv")
    """
    return newuser_origin, y_diff
