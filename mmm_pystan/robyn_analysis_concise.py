import pandas as pd
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import math
import hillfit

analysis_path_origin = "/Users/yanchunyang/Documents/datafiles/Rfile/advance/"
customized_folder = "20230101/"
analysis_path = analysis_path_origin + customized_folder
solID = '3_198_5'
optimal_spending_file = solID + "_reallocated_optimal.csv"
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

def compare_optimal_distribution(optimal_spending):

    ratio = optimal_spending.loc[optimal_spending['solID'] == solID, ['channels', 'initSpendShare', 'optmSpendShareUnit']]
    ratio.columns = ['rn', 'init_rate', 'suggest_rate']
    ratio_roi = pd.merge(ratio, metrics.loc[:, ['rn', 'roi_total', 'total_spend', 'total_response', 'mean_spend']], on=['rn'])

    ratio_roi['init_spending'] = ratio_roi['init_rate'] * nextspending
    ratio_roi['suggest_spending'] = ratio_roi['suggest_rate'] * nextspending

    ratio_roi['init_daily'] = ratio_roi['init_spending'] / nextday
    ratio_roi['suggest_daily'] = ratio_roi['suggest_spending']/ nextday

    ratio_roi = ratio_roi.apply(lambda x: calculate_margin_update(x), axis=1)

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

    ratio_roi = ratio_roi.apply(lambda x: calculate_margin_update(x), axis=1)

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

    return hf.x_fit, hf.y_fit, spending_response[tmp_channel + 'spending'], spending_response[tmp_channel]


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


def main():
    for item in colname:
        x_1, y_1, x_2, y_2 = get_spending_response(item)
        saturation_dic[item] = [x_1, y_1]
        original_dic[item] = [x_2, y_2]

    optimal_spending = pd.read_csv(analysis_path + solID + "_reallocated_best_1.csv")
    hist_spending = hist_spending = pd.read_csv(analysis_path + solID + "_reallocated_hist.csv")

    mean_spend = metrics['mean_spend']
    print(mean_spend)

    compare_optimal_distribution(optimal_spending)
    compare_hist_distribution(hist_spending)

    #channel_list = ['Apple_Search_Ads_iOS', 'Adwords_iOS', 'Facebook_Android']
    #channel_list = ['Snapchat_iOS', 'bytedanceglobal_int_Android','bytedanceglobal_int_iOS']
    #channel_list = [ 'Tatari_TV', 'Facebook_iOS', 'Adwords_Android',]
    #channel_list = [ 'new_channels']
    #channel_list = [ 'Tatari_TV', 'new_channels']
    #small_channels = ['Snapchat_iOS', 'bytedanceglobal_int_Android','bytedanceglobal_int_iOS', 'Snapchat_Android']
    channel_list = ['Apple_Search_Ads_iOS', 'Adwords_iOS', 'Facebook_Android', 'Facebook_iOS', 'Tatari_TV', 'new_channels', 'Adwords_Android']

    draw_saturtion_customized(channel_list)

if __name__ == '__main__':
    main()

