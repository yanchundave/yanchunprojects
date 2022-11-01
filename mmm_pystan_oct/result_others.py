"""
This file contains the graph functions which are not popular used. 
I save here in case of special requirements from managers.
"""

import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
from global_variable import *
from result_calculation import *
import seaborn as sns

def draw_roas_accumulative(roas_result):
    '''
    Plot the accumulative roas graphs for each channel
            Parameters:
                    roas_result (np.array): roas values for each channel
            Return:
                    No. Draw the roas graphs for each channel and save to folder.
    '''

    day_index = np.arange(1, 392)
    datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(origin_date))
    for i, item in enumerate(roas_result):
        ylimit = min(np.max(item) * 1.2, 1) 
        plt.subplot(1, 1, 1)
        plt.title("Spending Return for " + media_list[i])
        plt.ylim(0, ylimit)
        plt.plot(datetimes, item)
        plt.xlabel("time")
        plt.ylabel("new user acquired per dollor")   
        plt.savefig(datafile_path + "roas_accumulative" + media_list[i]+".png")
        plt.close()


def draw_roas_accumulative_all(roas_result):
    '''
    Plot the accumulative roas graph for all channels on same graph
            Parameters:
                    roas_result (np.array): roas values for each channel
            Return:
                    No. Draw the roas graph for all channels and save to folder.
    '''

    day_index = np.arange(1, Kl + 1)
    datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(origin_date))
    for i, item in enumerate(roas_result):
        plt.subplot(1,1,1)
        plt.plot(datetimes, item, label=media_list[i])
    plt.title("Spending Return" )
    plt.xlabel("time")
    plt.ylabel("new user acquired per dollor")
    plt.legend()
    plt.savefig(datafile_path + "roas_all_accumulative.png")
    plt.close()

def data_current_spending(df_parameter, mintotal, maxtotal, current_network):
    '''
    This function calculate the adstock mean and 2.5, 97.5 percentile of the current channel
            Parameters:
                    df_parameter (dataframe): training model result
                    mintotal (float): the current channel's minimal spending (used in MinMaxScaler)
                    maxtotal (float): the current chanenl's maximal spending (used in MinMaxScaler)
            Return:
                    values (list): the adstork reversed scaling tranform
    '''

    cols = ["x_hill." + str(i) + "." + str(current_network + 1) for i in range(1, Kl+1)]
    df_xhill = df_parameter.loc[:, cols].values
    x_hill_mean = np.mean(df_xhill, axis=0)
    x_hill_mean.sort()
    mean_value = np.mean(x_hill_mean) 
    left_limit = np.percentile(x_hill_mean, 2.5)
    right_limit = np.percentile(x_hill_mean, 97.5)
    return [x * (maxtotal - mintotal) + mintotal for x in [mean_value, left_limit, right_limit]]
    

def data_saturation(spending_origin, df_parameter):
    '''
    This function is to draw the saturation curve for all the channel. 
    The y-value is not the actual spending, but spending with carryover effect.
            Parameters:
                    spending_origin (np.array): spending without scaling
                    df_parameter (dataframe): training model result
            Return:
                    results (list): the spending's effect after applying carryover effect. Not revert to the new user, but log value
                    spending total (list): spending value for x axis in graph
                    current_spending
    '''

    beta_m_columns = ['beta_m.' + str(i) for i in range(1, Km + 1)]
    beta_m_values = df_parameter.loc[:, beta_m_columns].values
    beta_m = np.mean(beta_m_values, axis=0)

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
        spending_y = np.log(spending_scaler + 1)
        results.append(beta_m[i] * spending_y)
        network_current = data_current_spending(df_parameter, mintotal, maxtotal, i)
        current_spending.append(network_current)
    return results, spending_total, current_spending


def draw_saturation(results, spending_list, current_spending):
    '''
    Draw saturation curve for each channel. This one is with carryover effect.
            Parameters:
                    results (list): adstock effect 
                    spending_list: spending value for x axis.
                    current_spending: the current spending with carryover effect
            Return:
                    saturation curve for each channel
    '''

    for i in range(0, Km):
        plt.plot(spending_list[i], results[i], label=media_list[i])
        plt.axvline(current_spending[i][0], 0, 0.8, color = 'r', lw = 1, linestyle='--', label='mean')
        plt.axvline(current_spending[i][1], 0, 0.8, color = 'y', lw = 0.9, linestyle=':', label='CI 2.5')
        plt.axvline(current_spending[i][2], 0, 0.8, color = 'y', lw = 0.9, linestyle=':', label='CI 97.5')
        plt.text(current_spending[i][0], 0, "mean")
        plt.text(current_spending[i][1], 0, "P 2.5")
        plt.text(current_spending[i][2], 0, "P 97.5")
        plt.title("Saturation Curve and Daily Spending with Carryover Effect for " + media_list[i])
        plt.xlabel("accumulative spending")
        plt.ylabel("log(newuser acquired/1000)")
        plt.savefig(datafile_path + "Saturation Curve for " + media_list[i])
        plt.close()


def all_data_satuation(spending_origin, df_parameter):
    '''
    This function is to draw the saturation curve for all the channel. 
    The y-value is not the actual spending, but spending with carryover effect.
    To draw all the curves on the same graph, we have to align the spending axis, so we have to re-calculate the datasets.
            Parameters:
                    spending_origin (np.array): spending without scaling
                    df_parameter (dataframe): training model result
            Return:
                    results (list): the spending's effect after applying carryover effect. Not revert to the new user, but log value
                    spending total (list): spending value for x axis in graph
                    current_spending
    '''

    beta_m_columns = ['beta_m.' + str(i) for i in range(1, Km + 1)]
    beta_m_values = df_parameter.loc[:, beta_m_columns].values
    beta_m = np.mean(beta_m_values, axis=0)

    minvalue_each_network = np.min(spending_origin, axis=0)
    maxvalue_each_network = np.max(spending_origin, axis=0)

    minvalue = np.max(minvalue_each_network)
    maxvalue = np.max(maxvalue_each_network)
    spending_diff = (maxvalue * 3 - minvalue) / 1000
    spending_list = np.array([minvalue + t * spending_diff for t in range(0, 1000)])

    results = [] #y_value
    current_spending = []

    for i in range(0, Km):
        spending_scaler = (spending_list - minvalue_each_network[i]) / (maxvalue_each_network[i] - minvalue_each_network[i])
        spending_y = np.log(spending_scaler + 1)
        results.append(beta_m[i] * spending_y)
        network_current = data_current_spending(df_parameter, minvalue_each_network[i], maxvalue_each_network[i], i)
        current_spending.append(network_current)
    return results, spending_list, current_spending


def draw_effective_decay(df_parameter):
    '''
    Based on alpha and theta to draw one day spending effectiveness decay graph.
            Parameters:
                    df_parameter (dataframe): training model result
            Return:
                    Save decay graphs to folder. Only contains top three channels.
    '''

    alpha_cols = ['alpha.' + str(i) for i in range(1, 7)]
    theta_cols = ['theta.' + str(i) for i in range(1, 7)]
    alpha_value = df_parameter.loc[:, alpha_cols].values
    theta_value = df_parameter.loc[:, theta_cols].values
    alpha = np.mean(alpha_value, axis=0)
    theta = np.mean(theta_value, axis = 0)
    initial_x = 100
    t_set = np.arange(0, 7, 0.1)
    decay_set = []
    for i in range(0, Km):
        decay_set.append([initial_x * math.pow(alpha[i], (l - theta[i])**2) for l in t_set])
    plt.subplot(1,1,1)
    plt.plot(t_set, decay_set[0], label="Google spending effectiveness")
    plt.plot(t_set, decay_set[1], label="Apple spending effectiveness")
    plt.plot(t_set, decay_set[2], label="Facebook spending effectiveness")
    plt.axvline(2, 0, 0.8, color='r', linestyle=':')
    plt.plot(2, decay_set[0][20], marker="o", markersize=4, markerfacecolor="green")
    plt.plot(2, decay_set[1][20], marker="o", markersize=4, markerfacecolor="green")
    plt.plot(2, decay_set[2][20], marker="o", markersize=4, markerfacecolor="green")
    plt.text(2.1, decay_set[0][20], s=str('{0:.2f}'.format(decay_set[0][20])))
    plt.text(2.1, decay_set[1][20], s=str('{0:.2f}'.format(decay_set[1][20])))
    plt.text(2.1, decay_set[2][20], s=str('{0:.2f}'.format(decay_set[2][20])))
    plt.title("Daily Spending Effectiveness Decay")
    plt.legend()
    plt.savefig(datafile_path + "effective_decay.png")