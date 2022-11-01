from platform import platform
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from global_variable import *
from result_calculation import *
import seaborn as sns
from collections import defaultdict

"""
This file is to provide functions of graphs.
It is to support result_analysis.py
"""


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
The below function is to draw the decay effect: alpha value
"""

def draw_decay_effect(df_parameter, Km, media_list):
        '''
        Get the df_parameter and extract the alpha mean value to draw the bar chart for decay effect
        '''
        alpha_set = []
        alpha_columns = ['alpha.' + str(i) for i in range(1, Km + 1)]
        for item in alpha_columns:
                alpha_value = df_parameter[item].values[:]
        alpha_set.append(np.median(alpha_value))
        title = "Decay Ratio: Advance user" if FLAG == 1 else "Decay Ratio: Revenue"
        plt.figure(figsize=(11, 11))
        plt.subplot(1,1,1)
        plt.bar(media_list, alpha_set, width = 0.2)
        plt.xticks(rotation=-75)
        plt.xlabel("channel")
        plt.ylabel("decay ratio")
        plt.title(title)
        plt.savefig(datafile_path + "decay_effect.png")


"""
The below two functions are to plot parameters distribution
"""
def plot_distribution(df_parameter, Km, Kb):
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
        #theta_columns = ['theta.' + str(i) for i in range(1, Km + 1)]
        sigma_columns = ['sigma']
        ru_columns = ['ru']
        columns_set = [beta_b_columns, beta_m_columns, alpha_columns,  sigma_columns, ru_columns]
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

def draw_cac(roas_result, Km, Kl, media_list):
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

def draw_roas_remove(y_from_model, remove_result, Km, Kl, media_list):
        '''
        Draw the separated grahp for each the channel if its spending removed
                Parameters:
                        y_from_model (np.array): prediction from model if no channel spending is removed
                        remove_result: prediction from model if each channel spending is removed
                Return:
                        None. Draw the new users change after each channel spending removed and save to folder
        '''

        day_index = np.arange(1, Kl+1)
        datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(ORIGIN_DATE))
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


def draw_roas(roas_result, media_list):
        '''
        Plot the weekly roas graphs for each channel
                Parameters:
                        roas_result (dataframe): roas values for each channel
                Return:
                        No. Draw the roas graphs for each channel and save to folder.
        '''
        y_label = "new user acquired per dollar" if FLAG==1 else "revenue acquired per dollar spending"
        for i, item in enumerate(roas_result):
                plt.subplot(1, 1, 1)
                plt.title("Spending Return for " + media_list[i])
                plt.plot(item['datetime'].astype(str), item['weekly_roas'])
                plt.xticks(rotation=75)
                plt.xlabel("time")
                plt.ylabel(y_label)
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
        y_label = "new user acquired per dollar" if FLAG==1 else "revenue acquired per dollar spending"
        for i, item in enumerate(roas_result):
                plt.subplot(1,1,1)
                plt.plot(item['datetime'].astype(str), item['weekly_roas'], label=media[i])
        plt.title("Spending Return" )
        plt.xticks(rotation=75)
        plt.xlabel("time")
        plt.ylabel(y_label)
        plt.legend()
        plt.savefig(datafile_path + name + "_roas_all.png")
        plt.close()


"""
Original Spending Graph
"""

def draw_origin_spending(spending_origin, Km, media_list):

        """
                Draw a graph to show all the channels' spending
                        Parameters:
                                spending_origin. Directly read the spending_origin file.
                        Return:
                                None. All channels' spending graph is saved to folder
        """
        platform_list = ['iOS', 'Android']
        shape = spending_origin.shape
        day_index = np.arange(1, shape[0] + 1)
        datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(ORIGIN_DATE))

        y_sub = defaultdict(list)
        media_name = defaultdict(list)


        for i in range(Km):
                if platform_list[0] in media_list[i]:
                        y_sub[platform_list[0]].append(spending_origin[:,i])
                        media_name[platform_list[0]].append(media_list[i])
                elif platform_list[1] in media_list[i]:
                        y_sub[platform_list[1]].append(spending_origin[:,i])
                        media_name[platform_list[1]].append(media_list[i])
                else:
                        y_sub['other'].append(spending_origin[:,i])
                        media_name['other'].append(media_list[i])


        for item in ['iOS', 'Android', 'other']:
                plt.subplot(1,1,1)
                plt.title("Spending Distribution Among Networks " + item)
                platform_length = len(media_name[item])
                for i in range(0, platform_length):
                        plt.plot(datetimes, y_sub[item][i], label=media_name[item][i])
                plt.legend()
                plt.savefig(datafile_path + "spending_distribution_" + item +".png")
                plt.close()


"""
Draw all the saturation curves on the same graphs
"""

def draw_all_data_saturation(results, spending_list, current_spending, media_list, Km):
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


"""
Draw Saturation Curves based on platforms
"""
def draw_platform_data_saturation(results, spending_list, current_spending, media_list, Km):

        '''
        Draw saturation curve for each channel. This one is with carryover effect.
                Parameters:
                        results (list): adstock effect
                        spending_list: spending value for x axis.
                        current_spending: the current spending with carryover effect
                Return:
                        saturation curves for all channels
        '''
        y_label = "log(newuser/1000)" if FLAG==1 else "log(revenue/100000)"
        for item in ['iOS', 'Android', 'other']:
                title_name = "saturation_" + item + "png"
                for i in range(0, Km):
                        if item in media_list[i] or ('iOS' not in media_list[i] and 'Android' not in media_list[i]):
                                plt.plot(spending_list, results[i], label=media_list[i])
                                plt.scatter(current_spending[i][0], current_spending[i][1], s=10)

                plt.title("Saturation Graph ")
                plt.legend()
                plt.xlabel("accumulative spending")
                plt.ylabel(y_label)
                plt.savefig(datafile_path + title_name)
                plt.close()


"""
Draw Saturation Curve for each channel
"""
def draw_actual_saturation(results, spending_list, current_spending, media_list, Km):
        '''
        Draw each channel's saturation curve
                Parameters:
                        results (list): actual spending's effect from actual_data_saturation
                        spending_list (list): actual spending value on x axis
                        current_spending: the actual spending
                Return:
                        Save each channel's saturation curve to folders.
        '''
        y_label = "log(newuser/1000)" if FLAG==1 else "log(revenue/100000)"
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
                plt.ylabel(y_label)
                plt.savefig(datafile_path + "Saturation Curve for " + media_list[i])
                plt.close()


"""
Seasonality
"""
def draw_seaonality(df_parameter, df_basic, Kl):
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
        # moving average so need remove the first five points
        df_update = df_basic[5:-1, 4:6]
        day_index = np.arange(1, Kl+2)
        datetimes = pd.to_datetime(day_index, unit='D', origin=pd.Timestamp(ORIGIN_DATE))
        results = np.dot(season_coes, df_update.T)
        newuser = np.exp(results) * y_constant
        plt.subplot(1,1,1)
        plt.plot(datetimes, newuser)
        plt.xlabel("time")
        plt.ylabel("new user acquired")
        plt.title("seasonality effect")
        plt.savefig(datafile_path + "Seasonality_effect.png")


"""
    Graph combination function for roas
"""

def draw_channel_information(df_parameter, spending_origin, Km, Kl, T, media_list):
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

        y_from_model = y_fitted_from_bayesian(df_parameter, Kl)
        remove_result = []
        for i in range(1, Km+1):
                roas_columns = ["y_remove." + str(i) + "." + str(j) for j in range(1, Kl + 1)]
                y_values = df_parameter.loc[:, roas_columns].values
                remove_result.append(np.mean(y_values, axis=0))

        roas_result = []

        for i in range(0, Km):
                roas = get_roas(y_from_model, remove_result[i], spending_origin[:, i], Kl, T)
                roas_result.append(roas)

        draw_roas(roas_result, media_list)

        for item in ['iOS', 'Android', 'other']:
                media_name = []
                roas_sub = []
                for i in range(Km):
                        if item in media_list[i] or (item not in media_list[i] and item not in media_list[i]):
                                roas_sub.append(roas_result[i])
                                media_name.append(media_list[i])
                draw_roas_all(roas_sub, media_name, item)

"""
draw contribution
"""
def draw_contribution(total, contribution, media_list):
        total_sum = np.sum(total)
        channel_sum = np.sum(contribution, axis=0)
        ratio = channel_sum  * 1.0/ total_sum
        title = "Channel Contribution: Advance Userr" if FLAG== 1 else "Channel Contribution: Revenue"
        plt.figure(figsize=(11, 11))
        plt.subplot(1,1,1)
        plt.bar(media_list, ratio, width = 0.2)
        plt.xticks(rotation=-75)
        plt.xlabel("channel")
        plt.ylabel("contribution")
        plt.title(title)
        plt.savefig(datafile_path + "channel_contribution.png")