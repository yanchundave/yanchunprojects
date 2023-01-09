import pandas as pd
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import math
import hillfit
from scipy.optimize import minimize
from gekko import GEKKO

analysis_path_origin = "/Users/yanchunyang/Documents/datafiles/Rfile/advance/"
customized_folder = "workingfolder/"
analysis_path = analysis_path_origin + customized_folder
solID = '5_200_2'
optimal_spending_file = solID + "_reallocated.csv"
hist_spending_file = solID + "_reallocated_hist.csv"

# Original spendingd data
df = pd.read_csv("/Users/yanchunyang/Documents/datafiles/pystan/user/mmm_r_raw_one.csv")
# response decomposed by date
decompose = pd.read_csv(analysis_path + "pareto_alldecomp_matrix.csv")

# adstock spending and draw saturation curves
transform_file = pd.read_csv(analysis_path + "pareto_media_transform_matrix.csv", header=0)
# model summary metrics
metrics = pd.read_csv(analysis_path + "R_metrics_advance_1.csv")
# optimal spending suggestion and historic spending suggestion
#optimal_spending = pd.read_csv(analysis_path + optimal_spending_file)
#hist_spending = pd.read_csv(analysis_path + hist_spending_file)

colname = [
    'Snapchat_Android', 'bytedanceglobal_int_Android',
    'bytedanceglobal_int_iOS', 'Snapchat_iOS', 'Adwords_iOS',
    'Apple_Search_Ads_iOS', 'Tatari_TV', 'Facebook_Android', 'Facebook_iOS',
    'Adwords_Android',  'new_channels', 'minor_channels'
]

nextspending = 11000000
nextday = 90
historic_time_span = 295
# Robyn saturation curve x axle is hillfunction(adstockfunction(true spending)) so margin need be calculated separated
# Robyn mean_spending from metrics are the average mean spending without considering dates without spending
# We have to estimate the ROAS by decompse value
# need figure out a better way to estimate ROAS through smoothing technique, but temporarily solve by the below function
saturation_dic = {}
original_dic = {}
functionlist = []

def calculate_margin(tmp_channel, mean_spending):
    # this spending is adstorkedMedia
    # spending = transform_file.loc[(transform_file['type']== 'adstockedMedia')&(transform_file['solID'] == solID),:]
    # response = transform_file.loc[(transform_file['type']== 'decompMedia')&(transform_file['solID'] == solID),:]
    decom = decompose.loc[decompose['solID']== solID, ['ds', tmp_channel]]
    # decom and response are same so just take one
    # Response decompsition

    channel_daily_spending = df.loc[:, ['date', tmp_channel]]
    channel_daily_spending.columns = ['ds',tmp_channel + 'spending']

    dcom_supdate = pd.merge(channel_daily_spending, decom, on=['ds'], how='inner')
    # add 0.01 to avoid zero
    dcom_supdate['rate'] = dcom_supdate[tmp_channel] / (dcom_supdate[tmp_channel +'spending'] + 0.01)
    # Set the rate to zero if spending is zero
    dcom_supdate['rate'] = dcom_supdate.apply(lambda x: x['rate'] if x[tmp_channel+'spending'] > 0 else 0, axis=1)
    dcom_supdate = dcom_supdate.loc[dcom_supdate['rate']<=4,:]

    # Filter the setting spending left and right within 10%
    spendingchannelname = tmp_channel + 'spending'
    tupdate = dcom_supdate.loc[(dcom_supdate[spendingchannelname] > mean_spending * 0.9) & (dcom_supdate[spendingchannelname] < mean_spending * 1.1),:]

    tupdate = tupdate.sort_values(by=[tmp_channel +'spending'])
    margin = sum(tupdate[tmp_channel]) / (sum(tupdate[spendingchannelname])+0.01)
    return margin

def compare_optimal_distribution_optimal(optimal_spending):
    ratio = optimal_spending.loc[optimal_spending['solID'] == solID, ['channels', 'initSpendShare', 'optmSpendShareUnit']]
    ratio.columns = ['rn', 'init_rate', 'suggest_rate']
    ratio_roi = pd.merge(ratio, metrics.loc[:, ['rn', 'roi_total', 'total_spend', 'total_response', 'mean_spend']], on=['rn'])

    ratio_roi['init_spending'] = ratio_roi['init_rate'] * nextspending
    ratio_roi['suggest_spending'] = ratio_roi['suggest_rate'] * nextspending

    ratio_roi['init_daily'] = ratio_roi['init_spending'] / nextday
    ratio_roi['suggest_daily'] = ratio_roi['suggest_spending']/ nextday

    ratio_roi['init_roi'] = ratio_roi.apply(lambda x: calculate_margin(x['rn'], x['init_daily']), axis=1)
    ratio_roi['suggest_roi'] = ratio_roi.apply(lambda x: calculate_margin(x['rn'], x['suggest_daily']), axis=1)

    ratio_roi['init_daily_response'] = ratio_roi['init_roi'] * ratio_roi['init_daily']
    ratio_roi['suggest_daily_response'] = ratio_roi['suggest_roi'] * ratio_roi['suggest_daily']

    ratio_roi.to_csv(analysis_path + "ratio_roi_optimal.csv")

def compare_hist_distribution_optimal(hist_spending):
    ratio = hist_spending.loc[hist_spending['solID'] == solID,
                                 ['channels', 'initSpendShare', 'optmSpendShareUnit']]
    ratio.columns = ['rn', 'init_rate', 'suggest_rate']
    ratio_roi = pd.merge(ratio, metrics.loc[:, ['rn', 'mean_spend','roi_total', 'total_spend', 'total_response']], on=['rn'])
    spending = sum(ratio_roi['total_spend'])

    ratio_roi['suggest_spending'] = ratio_roi['suggest_rate'] * spending
    ratio_roi['suggest_daily'] = ratio_roi['suggest_spending'] / historic_time_span

    ratio_roi['init_roi'] = ratio_roi.apply(lambda x: calculate_margin(x['rn'], x['mean_spend']), axis=1)
    ratio_roi['suggest_roi'] = ratio_roi.apply(lambda x: calculate_margin(x['rn'], x['suggest_daily']), axis=1)

    # history to compare total instead of daily
    ratio_roi['init_response_total'] = ratio_roi['init_roi'] * ratio_roi['total_spend']
    ratio_roi['suggest_response_total'] = ratio_roi['suggest_roi'] * ratio_roi['suggest_spending']

    ratio_roi.to_csv(analysis_path + "ratio_roi_hist_optimal.csv")


def calculate_margin_update(row):
    tmp_channel = row['rn']
    mean_spending_init = row['init_daily']
    mean_spending_suggest = row['suggest_daily']

    points = saturation_dic[tmp_channel]
    x_1, y_1, x_2, y_2 = get_current_point(mean_spending_init, points[0], points[1])
    row['init_response'] = y_1
    row['init_margin'] = (y_2 - y_1)/(x_2 - x_1 + 0.01)

    x_1, y_1, x_2, y_2 = get_current_point(mean_spending_suggest, points[0], points[1])
    row['suggest_response'] = y_1
    row['suggest_margin'] = (y_2 - y_1)/(x_2 - x_1 + 0.01)

    return row

def get_hill_value(function_str, spending):
    x = spending
    y_1 = eval(function_str)
    x = spending + 100
    y_2 = eval(function_str)
    margin = (y_2 - y_1) / 100
    return y_1, margin

def calculate_margin_new(row):
    tmp_channel = row['rn']
    mean_spending_init = row['init_daily']
    mean_spending_suggest = row['suggest_daily']

    function_str = functionlist[tmp_channel].equation
    y_1, margin = get_hill_value(function_str, mean_spending_init)
    row['init_response'] = y_1
    row['init_margin'] = margin

    y_1_update, margin_update = get_hill_value(function_str, mean_spending_suggest)
    row['suggest_response'] = y_1_update
    row['suggest_margin'] = margin_update

    return row

def compare_optimal_distribution(optimal_spending):

    ratio = optimal_spending.loc[optimal_spending['solID'] == solID, ['channels', 'initSpendShare', 'optmSpendShareUnit']]
    ratio.columns = ['rn', 'init_rate', 'suggest_rate']
    ratio_roi = pd.merge(ratio, metrics.loc[:, ['rn', 'roi_total', 'total_spend', 'total_response', 'mean_spend']], on=['rn'])

    ratio_roi['init_spending'] = ratio_roi['init_rate'] * nextspending
    ratio_roi['suggest_spending'] = ratio_roi['suggest_rate'] * nextspending

    ratio_roi['init_daily'] = ratio_roi['init_spending'] / nextday
    ratio_roi['suggest_daily'] = ratio_roi['suggest_spending']/ nextday

    ratio_roi = ratio_roi.apply(lambda x: calculate_margin_new(x), axis=1)

    ratio_roi.to_csv(analysis_path + "ratio_roi.csv")

def compare_hist_distribution(hist_spending):

    ratio = hist_spending.loc[hist_spending['solID'] == solID,
                                 ['channels', 'initSpendShare', 'optmSpendShareUnit']]
    ratio.columns = ['rn', 'init_rate', 'suggest_rate']
    ratio_roi = pd.merge(ratio, metrics.loc[:, ['rn', 'mean_spend','roi_total', 'total_spend', 'total_response']], on=['rn'])
    spending = sum(ratio_roi['total_spend'])

    ratio_roi['suggest_spending'] = ratio_roi['suggest_rate'] * spending
    ratio_roi['suggest_daily'] = ratio_roi['suggest_spending'] / historic_time_span
    ratio_roi['init_daily'] = ratio_roi['mean_spend']

    ratio_roi = ratio_roi.apply(lambda x: calculate_margin_new(x), axis=1)

    ratio_roi.to_csv(analysis_path + "ratio_roi_hist.csv")


def get_current_point(x_1, x, y):
    for i in range(0, len(x)):
        if x[i] > x_1:
            return x_1, np.interp(x_1, [x[i-1], x[i]], [y[i-1], y[i]]), x[i], y[i]
    return x_1, 0, 1, 0


def get_spending_response(tmp_channel):
    real_spending = df.loc[:, ['date', tmp_channel]]
    real_spending.columns = ['ds',tmp_channel + 'spending']
    decom = decompose.loc[decompose['solID']== solID, ['ds', tmp_channel]]
    spending_response = pd.merge(real_spending, decom, on=['ds'], how='inner')
    #spending_response.loc[len(spending_response.index)] = ['2021-12-31', 0.1, 0]
    spending_response = spending_response.loc[spending_response[tmp_channel+'spending'] > 0,:]
    spending_response['rate'] = spending_response[tmp_channel] / (spending_response[tmp_channel + 'spending'])
    spending_response = spending_response.loc[spending_response['rate'] <4, :]
    spending_response = spending_response.sort_values(by=[tmp_channel + 'spending'])

    hf = hillfit.HillFit(spending_response[tmp_channel + 'spending'], spending_response[tmp_channel])
    hf.fitting(x_label='x', y_label='y', title='Fitted Hill equation', sigfigs=6, log_x=False, print_r_sqr=True,
           generate_figure=False, view_figure=False, export_directory=None, export_name=None)
    if hf.bottom < 0:
        hf = hillfit.HillFit(spending_response[tmp_channel + 'spending'], spending_response[tmp_channel], bottom_param=False)
        hf.fitting(x_label='x', y_label='y', title='Fitted Hill equation', sigfigs=6, log_x=False, print_r_sqr=True,
           generate_figure=False, view_figure=False, export_directory=None, export_name=None)

    return hf.x_fit, hf.y_fit, spending_response[tmp_channel + 'spending'], spending_response[tmp_channel], hf


# This is the customized saturation curve which used in calculation
def draw_saturtion_customized(channel_list):
    title = ""
    margin_dic = {}
    for item in channel_list:
        title += item + ", "
        x_0 = metrics.loc[metrics['rn'] == item, 'mean_spend'].values[0]
        x_1, y_1, x_2, y_2 = get_current_point(x_0, saturation_dic[item][0], saturation_dic[item][1])
        margin = (y_2 - y_1)/(x_2 - x_1 + 0.01)
        plt.plot(saturation_dic[item][0], saturation_dic[item][1],label=item, linewidth=1)
        #plt.plot(x_orig, y_orig, color='blue')
        plt.scatter(x_1, y_1)
        margin_dic[item] = margin
    plt.legend()
    plt.title("Saturation Curves of " + title)
    plt.gcf().set_size_inches(10, 5)
    plt.style.use('fast')
    plt.savefig(analysis_path + title.strip()+ ".png")
    plt.close()
    print(margin_dic)

# This is the official saturation curve
def draw_saturation(channel_list):
    spending = transform_file.loc[(transform_file['type']== 'adstockedMedia')&(transform_file['solID'] == solID),:]
    response = transform_file.loc[(transform_file['type']== 'decompMedia')&(transform_file['solID'] == solID),:]
    title = ""
    for item in channel_list:
        title += item + ", "
        x = spending.loc[:,['ds',item]]
        y = response.loc[:,['ds',item]]
        xy = pd.merge(x, y, on=['ds'])
        xy = xy.sort_values(by=[item+'_x'])

        x_1 = spending[item].mean()
        y_1 = response[item].mean()

        plt.plot(xy[item+'_x'], xy[item+'_y'], label=item, linewidth=1)

        plt.scatter(x_1, y_1)
    plt.legend()
    plt.title("Saturation Curves of " + title)
    plt.gcf().set_size_inches(10, 5)
    plt.style.use('fast')
    plt.savefig(analysis_path + title.strip()+ ".png")

def get_objective(spending_list):
    objective = 0
    for i, item in enumerate(colname[:-1]):
        objective += eval(functionlist[i].equation, {"x": spending_list[i]})
    return objective

def get_marginal(spending_list):
    marginal_list = []
    for i, item in enumerate(colname[:-1]):
        response_1 = eval(functionlist[i].equation, {"x": spending_list[i]})
        response_2 = eval(functionlist[i].equation, {"x": spending_list[i] + 1000})
        marginal_list.append((response_2 - response_1) / 1000)
    marginal_list.append(0)
    return marginal_list

def compare_optimal_gekko(optimal_spending):

    simulation_dic = {}
    ratio = optimal_spending.loc[optimal_spending['solID'] == solID, ['channels', 'initSpendShare', 'optmSpendShareUnit']]
    ratio.columns = ['rn', 'init_rate', 'suggest_rate']
    ratio_roi = pd.merge(ratio, metrics.loc[:, ['rn', 'roi_total', 'total_spend', 'total_response', 'mean_spend']], on=['rn'])

    ratio_roi['init_spending'] = ratio_roi['init_rate'] * nextspending
    ratio_roi['init_daily'] = ratio_roi['init_spending'] / nextday
    spending_check = []

    for item in colname[:-1]:
        spending_check.append(ratio_roi.loc[ratio_roi['rn'] == item, 'init_daily'].values[0])

    simulation_dic['initial'] = spending_check + [get_objective(spending_check)]
    simulation_dic['20'] = optimizer_solver(spending_check, 0.8, 1.2)
    simulation_dic['30'] = optimizer_solver(spending_check, 0.7, 1.3)
    simulation_dic['50'] = optimizer_solver(spending_check, 0.5, 1.5)
    simulation_dic['80'] = optimizer_solver(spending_check, 0.2, 1.8)

    simulation_dic['30_margin']= get_marginal(simulation_dic['30'][:-1])

    df_result = pd.DataFrame(simulation_dic, index=colname[:-1] + ['objective'])
    df_result.to_csv(analysis_path + "df_result_optimal.csv")

def optimizer_solver(initalspending, lowerbound, upperbound):
    m = GEKKO()
    #x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12 = m.Array(m.Var, 12, lb=1000, ub=122222)
    x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11= m.Array(m.Var, 11, lb=100, ub=122222)
    #m.Equation(x1 + x2 + x3 + x4 + x5 + x6 + x7 + x8 + x9 + x10 + x11 + x12 ==122222)

    m.Equation(x1 + x2 + x3 + x4 + x5 + x6 + x7 + x8 + x9 + x10 + x11 ==122222)

    m.Equation(x1 >= initalspending[0] * lowerbound)
    m.Equation(x2 >= initalspending[1] * lowerbound)
    m.Equation(x3 >= initalspending[2] * lowerbound)
    m.Equation(x4 >= initalspending[3] * lowerbound)
    m.Equation(x5 >= initalspending[4] * lowerbound)
    m.Equation(x6 >= initalspending[5] * lowerbound)
    m.Equation(x7 >= initalspending[6] * lowerbound)
    m.Equation(x8 >= initalspending[7] * lowerbound)
    m.Equation(x9 >= initalspending[8] * lowerbound)
    m.Equation(x10 >= initalspending[9] * lowerbound)
    m.Equation(x11 >= initalspending[10] * lowerbound)

    m.Equation(x1 <= initalspending[0] * upperbound)
    m.Equation(x2 <= initalspending[1] * upperbound)
    m.Equation(x3 <= initalspending[2] * upperbound)
    m.Equation(x4 <= initalspending[3] * upperbound)
    m.Equation(x5 <= initalspending[4] * upperbound)
    m.Equation(x6 <= initalspending[5] * upperbound)
    m.Equation(x7 <= initalspending[6] * upperbound)
    m.Equation(x8 <= initalspending[7] * upperbound)
    m.Equation(x9 <= initalspending[8] * upperbound)
    m.Equation(x10 <= initalspending[9] * upperbound)
    m.Equation(x11 <= initalspending[10] * upperbound)

    #for i in range(1, 4):
        #m.Equation(eval('x' + str(i) + '>= initalspending[i-1] * 0.7'))
        #m.Equation(eval('x' + str(i) + '<= initalspending[i-1] * 1.3'))

    """
    m.Maximize(eval(functionlist[0].equation, {"x": x1}) +
    eval(functionlist[1].equation, {"x": x2}) +
    eval(functionlist[2].equation, {"x": x3}) +
    eval(functionlist[3].equation, {"x": x4}) +
    eval(functionlist[4].equation, {"x": x5}) +
    eval(functionlist[5].equation, {"x": x6}) +
    eval(functionlist[6].equation, {"x": x7}) +
    eval(functionlist[7].equation, {"x": x8}) +
    eval(functionlist[8].equation, {"x": x9}) +
    eval(functionlist[9].equation, {"x": x10}) +
    eval(functionlist[10].equation, {"x": x11}) +
    eval(functionlist[11].equation, {"x": x12})
    )
    """
    m.Maximize(eval(functionlist[0].equation, {"x": x1}) +
    eval(functionlist[1].equation, {"x": x2}) +
    eval(functionlist[2].equation, {"x": x3}) +
    eval(functionlist[3].equation, {"x": x4}) +
    eval(functionlist[4].equation, {"x": x5}) +
    eval(functionlist[5].equation, {"x": x6}) +
     eval(functionlist[6].equation, {"x": x7}) +
    eval(functionlist[7].equation, {"x": x8}) +
    eval(functionlist[8].equation, {"x": x9}) +
    eval(functionlist[9].equation, {"x": x10}) +
     eval(functionlist[10].equation, {"x": x11})
    )
    #m.options.SOLVER=1
    m.options.MAX_ITER=10000
    #m.options.COLDSTART=1

    m.solve(disp=True)

    print(x1.value)
    print(x2.value)
    print(x3.value)
    print(x4.value)
    print(x5.value)
    print(x6.value)
    print(x7.value)
    print(x8.value)
    print(x9.value)
    print(x10.value)
    print(x11.value)
    print('Objective: ' + str(-1*m.options.objfcnval))

    x = [
        x1.value[0],
        x2.value[0],
        x3.value[0],
        x4.value[0],
        x5.value[0],
        x6.value[0],
        x7.value[0],
        x8.value[0],
        x9.value[0],
        x10.value[0],
        x11.value[0],
        -1*m.options.objfcnval
    ]
    print("here")
    return x


def main():
    for item in colname:
        if item == 'minor_channels':
            continue
        x_1, y_1, x_2, y_2, func= get_spending_response(item)
        saturation_dic[item] = [x_1, y_1]
        original_dic[item] = [x_2, y_2]
        functionlist.append(func)

    for i, key in enumerate(colname[:-1]):
        print(key)
        print(functionlist[i].bottom)


    optimal_spending = pd.read_csv(analysis_path + solID + "_reallocated.csv")
    hist_spending = hist_spending = pd.read_csv(analysis_path + solID + "_reallocated_hist.csv")

    mean_spend = metrics['mean_spend']
    print(mean_spend)

    compare_optimal_gekko(optimal_spending)
    #compare_hist_distribution(hist_spending)

    #compare_optimal_distribution_optimal(optimal_spending)
    #compare_hist_distribution_optimal(hist_spending)

    #channel_list = ['Apple_Search_Ads_iOS', 'Adwords_iOS', 'Facebook_Android']
    #channel_list = ['Snapchat_iOS', 'bytedanceglobal_int_Android','bytedanceglobal_int_iOS']
    #channel_list = [ 'Tatari_TV', 'Facebook_iOS', 'Adwords_Android',]
    #channel_list = [ 'new_channels']
    #channel_list = [ 'Tatari_TV', 'new_channels']
    #small_channels = ['Snapchat_iOS', 'bytedanceglobal_int_Android','bytedanceglobal_int_iOS', 'Snapchat_Android']
    #channel_list = ['Apple_Search_Ads_iOS', 'Adwords_iOS', 'Facebook_Android', 'Facebook_iOS', 'Tatari_TV', 'new_channels', 'Adwords_Android']

    #draw_saturtion_customized(channel_list)

if __name__ == '__main__':
    main()

