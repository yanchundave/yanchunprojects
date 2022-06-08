"""
This file is to provide functions of graphs. 
It is to support result_analysis.py
"""

import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
from global_variable import *
from result_calculation import *
import seaborn as sns

def draw_graph_name(a, b, a_name, b_name):
    '''
    Return a plot of two lists with two list name.

            Parameters:
                    a (list): value list a
                    b (list): value list b
                    a_name (string): the name of list a
                    b_name (string): the name of list b
            Return:
                    png graph with two-line chart
    '''
    
    plt.plot(a, label=a_name)
    plt.plot(b, label=b_name)
    plt.legend()
    plt.savefig(datafile_path + a_name + "_" + b_name+".png")
    plt.close()

"""
The below two functions are to plot parameters distribution
"""
def plot_distribution(df_parameter):
    '''
    Get the parameter distribution and call Plot function to draw their distributions of beta, alpha and  theta
            Parameters:
                    df_parameter (dataframe): training model result
            Return:
                    None. Plot the graphs of all parameters
    '''

    beta_b_columns = ['beta_b.' + str(i) for i in range(1, Kb + 1)]
    beta_m_columns = ['beta_m.' + str(i) for i in range(1, Km + 1)]
    alpha_columns = ['alpha.' + str(i) for i in range(1, Km + 1)]
    theta_columns = ['theta.' + str(i) for i in range(1, Km + 1)]
    sigma_columns = ['sigma']
    ru_columns = ['ru']
    columns_set = [beta_b_columns, beta_m_columns, alpha_columns, theta_columns, sigma_columns, ru_columns]
    for col in columns_set:
        for item in col:
            plot_trace(df_parameter[item].values[:], item)


def plot_trace(param, param_name='par_name'):
    '''
    Draw the parameter distribution based on the sample values and name.
            Parameters:
                    param (np.array): parameter's sample value list
                    param_name (string): parameter's name
            Return:
                    None. Plot the graphs of specific parameter and save to folder
    '''

    mean = np.mean(param)
    median = np.median(param)
    cred_min, cred_max = np.percentile(param, 2.5), np.percentile(param, 97.5)
    
    plt.subplot(2,1,1)
    plt.plot(param)
    plt.xlabel('samples')
    plt.ylabel(param_name)
    plt.axhline(mean, color='r', lw=2, linestyle='--')
    plt.axhline(median, color='c', lw=2, linestyle='--') 
    plt.axhline(cred_min, linestyle=':', color='k', alpha=0.2) 
    plt.axhline(cred_max, linestyle=':', color='k', alpha=0.2) 
    plt.title('Trace and Posterior Distribution for {}'.format(param_name))
    plt.subplot(2,1,2)
    plt.hist(param, 60, density=True); 
    sns.kdeplot(param, shade=True) 
    plt.xlabel(param_name)
    plt.ylabel('density')
    plt.axvline(mean, color='r', lw=2, linestyle='--',label='mean') 
    plt.axvline(median, color='c', lw=2, linestyle='--',label='median') 
    plt.axvline(cred_min, linestyle=':', color='k', alpha=0.2, label='95% CI') 
    plt.axvline(cred_max, linestyle=':', color='k', alpha=0.2)
    plt.legend()
    plt.savefig(datafile_path + param_name+".png")
    plt.close()


"""
Draw ROAS, CAS related functions
"""

def draw_cac(roas_result):
    '''
    Plot the cac graph for all the channels. This function is required by presentation. Convert ROAS to CAC.
            Parameters:
                    roas_result (np.array): all channels' roas value
            Return:
                    None. Draw all channels' CAC graph and save.
    '''

    roas_mean = [np.mean([x for x in roas_result[i]['weekly_roas'].values if ~np.isnan(x) and x > 0 and x <=1]) for i in range(0, Km)]
    print(roas_mean)
    cac = [int(1/x) for x in roas_mean]
    plt.subplot(1,1,1)
    plt.bar(media_list, cac, color='green', width=0.2)
    plt.xticks(rotation=-10)
    plt.savefig(datafile_path + "cac.png")

def draw_roas_remove(y_from_model, remove_result):
    '''
    Draw the separated grahp for each the channel if its spending removed
            Parameters:
                    y_from_model (np.array): prediction from model if no channel spending is removed
                    remove_result: prediction from model if each channel spending is removed
            Return:
                    None. Draw the new users change after each channel spending removed and save to folder
    '''

    day_index = np.arange(1, Kl+1)
    datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(origin_date))
    rates = []
    for i in range(0, Km):
        newuser_origin = np.exp(y_from_model) * 1000
        newuser_remove = np.exp(remove_result[i]) * 1000
        rates.append(calcualte_droping_rate(newuser_origin, newuser_remove))
        plt.plot(datetimes, newuser_origin, label="actual new users")
        plt.plot(datetimes, newuser_remove, label="predicted new users after removing media " + media_list[i])
        plt.legend()
        plt.savefig(datafile_path + "origin" + "_" + media_list[i] +".png")
        plt.close()
    print(rates) # Need reconsider here


def draw_roas(roas_result):
    '''
    Plot the weekly roas graphs for each channel
            Parameters:
                    roas_result (dataframe): roas values for each channel
            Return:
                    No. Draw the roas graphs for each channel and save to folder.
    '''

    for i, item in enumerate(roas_result):
        plt.subplot(1, 1, 1)
        plt.title("Spending Return for " + media_list[i])
        plt.plot(item['datetime'], item['weekly_roas'])
        plt.xlabel("time")
        plt.ylabel("new user acquired per dollor")
        plt.savefig(datafile_path + "roas_" + media_list[i]+".png")
        plt.close()


def draw_roas_all(roas_result, media, name):
    '''
    Plot the weekly roas graph for all channels on same graph
            Parameters:
                    roas_result (dataframe): roas values for each channel
            Return:
                    No. Draw the roas graph for all channels and save to folder.
    '''

    for i, item in enumerate(roas_result):
        plt.subplot(1,1,1)
        plt.plot(item['datetime'], item['weekly_roas'], label=media[i])
    plt.title("Spending Return" )
    plt.xlabel("time")
    plt.ylabel("new user acquired per dollor")
    plt.legend()
    plt.savefig(datafile_path + name + "_roas_all.png")
    plt.close()


    """
    Original Spending Graph
    """

def draw_origin_spending(spending_origin):
    """
        Draw a graph to show all the channels' spending
                Parameters:
                        spending_origin. Directly read the spending_origin file.
                Return:
                        None. All channels' spending graph is saved to folder
    """ 
    shape = spending_origin.shape   
    day_index = np.arange(1, shape[0] + 1)
    datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(origin_date))
    plt.subplot(1,1,1)
    plt.title("Spending Distribution Among Networks")
    for i in range(0, Km):
        plt.plot(datetimes, spending_origin[:, i], label=media_list[i])
    plt.legend()
    plt.savefig(datafile_path + "spending_distribution.png")
    plt.close()


"""
"""

def draw_all_data_saturation(results, spending_list, current_spending):
    '''
    Draw saturation curve for each channel. This one is with carryover effect.
            Parameters:
                    results (list): adstock effect 
                    spending_list: spending value for x axis.
                    current_spending: the current spending with carryover effect
            Return:
                    saturation curves for all channels
    '''
    for i in range(0, Km):
        plt.plot(spending_list, results[i], label=media_list[i])
        plt.scatter(current_spending[i][0], current_spending[i][1], s=10)
        
    plt.title("Saturation Graph ")
    plt.legend()
    plt.xlabel("accumulative spending")
    plt.ylabel("log(newuser acquired/1000)")
    plt.savefig(datafile_path + "saturation_total.png")
    plt.close()


def draw_actual_saturation(results, spending_list, current_spending):
    '''
    Draw each channel's saturation curve
            Parameters:
                    results (list): actual spending's effect from actual_data_saturation
                    spending_list (list): actual spending value on x axis
                    current_spending: the actual spending
            Return:
                    Save each channel's saturation curve to folders.
    '''

    for i in range(0, Km):
        plt.plot(spending_list[i], results[i], label=media_list[i])
        plt.axvline(current_spending[i][0], 0, 0.8, color = 'r', lw = 1, linestyle='--', label='mean')
        plt.axvline(current_spending[i][1], 0, 0.8, color = 'y', lw = 0.9, linestyle=':', label='CI 2.5')
        plt.axvline(current_spending[i][2], 0, 0.8, color = 'y', lw = 0.9, linestyle=':', label='CI 97.5')
        plt.text(current_spending[i][0], 0, "mean")
        plt.text(current_spending[i][1], 0, "P 2.5")
        plt.text(current_spending[i][2], 0, "P 97.5")
        plt.title("Saturation Curve and Daily Spending for " + media_list[i])
        plt.xlabel("Spending")
        plt.ylabel("log(newuser acquired/1000)")
        plt.savefig(datafile_path + "Saturation Curve for " + media_list[i])
        plt.close()


"""
Seasonality
"""
def draw_seaonality(df_parameter, df_basic):
    '''
    Draw seasonaly effect from model result
            Parameters: 
                    df_parameter (dataframe): training model result
                    df_basic (np.array): the basic variable values
            Return:
                    save the seasonality graph into the folder
    '''

    season_cols = ['beta_b.5', 'beta_b.6']
    season_values = df_parameter.loc[:, season_cols].values
    season_coes = np.mean(season_values, axis=0)
    df_update = df_basic[5:-1, 4:6]
    day_index = np.arange(1, 393)
    datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(origin_date))
    results = np.dot(season_coes, df_update.T)
    newuser = np.exp(results) * 1000
    plt.subplot(1,1,1)
    plt.plot(datetimes, newuser)
    plt.xlabel("time")
    plt.ylabel("new user acquired")
    plt.title("seasonality effect")
    plt.savefig(datafile_path + "Seasonality_effect.png")


"""
    Graph combination function for roas
"""

def draw_channel_information(df_parameter, spending_origin, flag=1):
    '''
    This function 
            1. prepares the data for calcuating the two roas (weekly and accumulative)
            2. call the functions to calculate the roas
            3. call the functions to draw the roas
            4. call cac functions if flag = 1
            Parameters:
                    df_parameter (dataframe): training model result
                    spending_origin (np.array): spending without scaling
                    flag (int): if flag == 1 then call weekly roas, otherwise call accumulative roas
            Return:
                    None. Call draw functions to obtain the graphs of roas for each channel
    '''

    y_from_model = y_fitted_from_bayesian(df_parameter)
    remove_result = []
    for i in range(1, Km+1):
        roas_columns = ["y_remove." + str(i) + "." + str(j) for j in range(1, Kl + 1)]
        y_values = df_parameter.loc[:, roas_columns].values
        remove_result.append(np.mean(y_values, axis=0))
    
    roas_result = []
 
    for i in range(0, Km):
        roas = get_roas(y_from_model, remove_result[i], spending_origin[:, i])
        roas_result.append(roas)
   
    draw_roas(roas_result)

    group_name = ['iOS', 'Android']
    #group_name = ['Adwords', 'Apple', 'Facebook', 'Snapchat', 'bytedanceglobal', 'unknown', 'TV']
    for item in group_name:
        media_name = []
        roas_sub = []
        for i in range(Km):
            if item in media_list[i]:
                roas_sub.append(roas_result[i])
                media_name.append(media_list[i])
        draw_roas_all(roas_sub, media_name, item)
    
    media_name = []
    roas_sub = []
    for i in range(Km):
        sign = 0
        for item in group_name:
            if item in media_list[i]:
                sign = 1
                break
        if sign == 0:
            media_name.append(media_list[i])
            roas_sub.append(roas_result[i])
    draw_roas_all(roas_sub, media_name, "others")
