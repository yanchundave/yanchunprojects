import pandas as pd
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import math
import hillfit

analysis_path = "/Users/yanchunyang/Documents/datafiles/Rfile/advance/"

# Original spendingd data
df = pd.read_csv("/Users/yanchunyang/Documents/datafiles/pystan/user/mmm_r_raw_one.csv")
# response decomposed by date
decompose = pd.read_csv(analysis_path + "pareto_alldecomp_matrix.csv")

# adstock spending and draw saturation curves
transform_file = pd.read_csv(analysis_path + "pareto_media_transform_matrix.csv", header=0)
# model summary metrics
metrics = pd.read_csv(analysis_path + "R_metrics_advance_1.csv")
# optimal spending suggestion and historic spending suggestion
optimal_spending = pd.read_csv(analysis_path + "5_200_2_reallocated_optimal.csv")
hist_spending = pd.read_csv(analysis_path + "5_200_2_reallocated_hist.csv")

colname = [
    'Snapchat_Android', 'bytedanceglobal_int_Android',
    'bytedanceglobal_int_iOS', 'Snapchat_iOS', 'Adwords_iOS',
    'Apple_Search_Ads_iOS', 'Tatari_TV', 'Facebook_Android', 'Facebook_iOS',
    'Adwords_Android',  'new_channels'
]

solID = '5_200_2'
nextspending = 11000000
nextday = 90
historic_time_span = 295
# Robyn saturation curve x axle is hillfunction(adstockfunction(true spending)) so margin need be calculated separated
# Robyn mean_spending from metrics are the average mean spending without considering dates without spending
# We have to estimate the ROAS by decompse value
# need figure out a better way to estimate ROAS through smoothing technique, but temporarily solve by the below function

def moving_average(a, n):
    '''
    Return the moving average of a list based on the moving number.

            Parameters:
                    a (list): the list for moving average
                    n (integer): the number for moving avearge
    '''

    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n-1:] / n

def get_margin(tmp_channel, mean_spending, k):
    p = simuation_function(tmp_channel)
    mean_spending_rate1 = p(mean_spending)
    mean_spending_rate2 = p(mean_spending + k)
    r2 = (mean_spending + k) * mean_spending_rate2
    r1 = mean_spending * mean_spending_rate1
    #print(mean_spending_rate1)
    #print(mean_spending_rate2)
    return (r2 - r1) / k


def get_margin_update(tmp_channel, mean_spending, k):

    mean_spending_rate1 = calculate_margin(tmp_channel, mean_spending)
    mean_spending_rate2 = calculate_margin(tmp_channel, mean_spending + k)
    r2 = (mean_spending + k) * mean_spending_rate2
    r1 = mean_spending * mean_spending_rate1
    print(mean_spending_rate1)
    print(mean_spending_rate2)
    print(r2)
    print(r1)
    #print(mean_spending_rate1)
    #print(mean_spending_rate2)
    return (r2 - r1) / k

def sigmoid(x, L ,x0, k, b):
    y = L / (1 + np.exp(-k*(x-x0))) + b
    return (y)

def hillfunc(x, a, b, c, d):
    y = a + (b-a)/(1 + math.power(c/x, d))
    return y

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

def compare_optimal_distribution(optimal_spending):
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

    ratio_roi.to_csv(analysis_path + "ratio_roi.csv")

def compare_hist_distribution(hist_spending):
    ratio = hist_spending.loc[hist_spending['solID'] == '5_200_2',
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

    ratio_roi.to_csv(analysis_path + "ratio_roi_hist.csv")

def get_point(minspend, maxspend, a, b, c, d):
    cumx = minspend
    cumy = 0
    x = [0]
    y = [0]
    dis = int((maxspend - minspend) / 100)
    for i in range(0, 100):
        cumx += dis
        rate = func1(cumx, a, b, c, d)
        if rate < 0:
            print(cumx)
        cumy += func1(cumx, a, b, c, d) * dis
        if cumy > 0:
            x.append(cumx)
            y.append(cumy)
    return x[1:], y[1:]

def get_point_update(minspend, maxspend, p):
    cumx = minspend
    cumy = 0
    x = [0]
    y = [0]
    dis = int((maxspend - minspend) / 200)
    for i in range(0, 100):
        cumx += dis
        rate = p(cumx)
        if rate < 0:
            print(cumx)
        cumy = rate * cumx
        if cumy > 0:
            x.append(cumx)
            y.append(cumy)
    return x[1:], y[1:]

def get_current_point(x_1, x, y):
    for i in range(0, len(x)):
        if x[i] > x_1:
            return x_1, np.interp(x_1, [x[i-1], x[i]], [y[i-1], y[i]]), x[i], y[i]
    return x_1, 0, 1, 0

def draw_spending_response(tmp_channel):
    real_spending = df.loc[:, ['date', tmp_channel]]
    real_spending.columns = ['ds',tmp_channel + 'spending']
    decom = decompose.loc[decompose['solID']== solID, ['ds', tmp_channel]]
    spending_response = pd.merge(real_spending, decom, on=['ds'], how='inner')
    #spending_response.loc[len(spending_response.index)] = ['2021-12-31', 0.1, 0]
    spending_response = spending_response.loc[spending_response[tmp_channel+'spending'] > 0,:]
    spending_response['rate'] = spending_response[tmp_channel] / (spending_response[tmp_channel + 'spending'])
    spending_response = spending_response.loc[spending_response['rate'] <4, :]
    spending_response = spending_response.sort_values(by=[tmp_channel + 'spending'])
    p = func2(spending_response[tmp_channel+'spending'], spending_response[tmp_channel], 2)
    spending_response['yhat'] = p(spending_response[tmp_channel + 'spending'])
    hf = hillfit.HillFit(spending_response[tmp_channel + 'spending'], spending_response['yhat'])
    hf.fitting(x_label='x', y_label='y', title='Fitted Hill equation', sigfigs=6, log_x=False, print_r_sqr=True,
           generate_figure=False, view_figure=True, export_directory=None, export_name=None)
    x_0 = metrics.loc[metrics['rn'] == tmp_channel, 'mean_spend'].values[0]
    x_1, y_1, x_2, y_2 = get_current_point(x_0, hf.x_fit, hf.y_fit)
    #plt.plot(spending_response[tmp_channel + 'spending'], spending_response[tmp_channel])
    return hf.x_fit, hf.y_fit, x_1, y_1, x_2, y_2

def draw_roas_rate(tmp_channel):
    real_spending = df.loc[:, ['date', tmp_channel]]
    real_spending.columns = ['ds',tmp_channel + 'spending']
    decom = decompose.loc[decompose['solID']== solID, ['ds', tmp_channel]]
    spending_response = pd.merge(real_spending, decom, on=['ds'], how='inner')
    spending_response = spending_response.loc[spending_response[tmp_channel+'spending'] > 0,:]
    spending_response['rate'] = spending_response[tmp_channel] / (spending_response[tmp_channel + 'spending'])
    spending_response = spending_response.loc[spending_response['rate'] <4, :]
    # get break even point
    filtered_spending = spending_response.loc[(spending_response['rate'] >=0.9) & (spending_response['rate'] <= 1.1), :]

    spending_needed = filtered_spending[tmp_channel+'spending'].mean()
    #print(spending_needed)
    spending_response = spending_response.sort_values(by=[tmp_channel + 'spending'])
    """
    params, _ = curve_fit(func1, spending_response[tmp_channel+'spending'], spending_response['rate'])
    a, b, c, d= params[0], params[1], params[2], params[3]
    maxspend = spending_response[tmp_channel+'spending'].max()
    minspend = spending_response[tmp_channel+'spending'].min()
    x, y = get_point(minspend, maxspend, a, b, c, d)
    yfit1 = func1(spending_response[tmp_channel+'spending'], a, b, c, d)
    #ypredict = spending_response[tmp_channel+'spending'] * yfit1
    """
    p = func2(spending_response[tmp_channel+'spending'], spending_response['rate'], 7)
    yhat = p(spending_response[tmp_channel+'spending'])

    maxspend = spending_response[tmp_channel+'spending'].max()
    minspend = spending_response[tmp_channel+'spending'].min()
    x, y = get_point_update(minspend, maxspend, p)

    #yfit1 = func1(spending_response[tmp_channel+'spending'], a, b, c, d)
    #plt.plot(spending_response[tmp_channel+'spending'], spending_response['rate'])
    #plt.plot(spending_response[tmp_channel+'spending'], ypredict)
    #plt.plot(spending_response[tmp_channel+'spending'], yhat)
    #plt.savefig(analysis_path + tmp_channel + '_rate.png')
    #plt.close()
    #return spending_needed
    x_0 = metrics.loc[metrics['rn'] == tmp_channel, 'mean_spend'].values[0]
    x_1, y_1, x_2, y_2 = get_current_point(x_0, x, y)
    return x, y, x_1, y_1, x_2, y_2

def func1(x, a, b, c, d):
    return a*x**3+ b*x**2 + c*x + d

def func2(x, y, degree):
    coeffs = np.polyfit(x, y, degree)
    p = np.poly1d(coeffs)
    return p

def draw_saturtion_customized(channel_list):
    title = ""
    margin_dic = {}
    for item in channel_list:
        title += item + ", "
        x, y, x_1, y_1, x_2, y_2 = draw_spending_response(item)
        margin = (y_2 - y_1)/(x_2 - x_1 + 0.01)
        plt.plot(x, y,label=item, linewidth=1)
        plt.scatter(x_1, y_1)
        margin_dic[item] = margin
    plt.legend()
    plt.title("Saturation Curves of " + title)
    plt.gcf().set_size_inches(10, 5)
    plt.style.use('fast')
    plt.savefig(analysis_path + title.strip()+ ".png")
    plt.close()
    print(margin_dic)

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
        """
        x_1 = metrics.loc[metrics['rn'] == item, 'mean_spend'].values[0]
        y_1 = metrics.loc[metrics['rn'] == item, 'mean_response'].values[0]
        """
        plt.plot(xy[item+'_x'], xy[item+'_y'], label=item, linewidth=1)
        #plt.plot(xy[item+'_x'], yfit1, label=item, linewidth=1, color='green')

        plt.scatter(x_1, y_1)
    plt.legend()
    plt.title("Saturation Curves of " + title)
    plt.gcf().set_size_inches(10, 5)
    plt.style.use('fast')
    plt.savefig(analysis_path + title.strip()+ ".png")


def simuation_function(tmp_channel):
    real_spending = df.loc[:, ['date', tmp_channel]]
    real_spending.columns = ['ds',tmp_channel + 'spending']
    decom = decompose.loc[decompose['solID']== solID, ['ds', tmp_channel]]
    spending_response = pd.merge(real_spending, decom, on=['ds'], how='inner')
    spending_response = spending_response.loc[spending_response[tmp_channel+'spending'] > 0,:]
    spending_response['rate'] = spending_response[tmp_channel] / (spending_response[tmp_channel + 'spending'])
    spending_response = spending_response.loc[spending_response['rate'] <4, :]
    # get break even point
    filtered_spending = spending_response.loc[(spending_response['rate'] >=0.9) & (spending_response['rate'] <= 1.1), :]

    spending_needed = filtered_spending[tmp_channel+'spending'].mean()
    #print(spending_needed)
    spending_response = spending_response.sort_values(by=[tmp_channel + 'spending'])
    """
    params, _ = curve_fit(func1, spending_response[tmp_channel+'spending'], spending_response['rate'])
    a, b, c, d= params[0], params[1], params[2], params[3]
    maxspend = spending_response[tmp_channel+'spending'].max()
    minspend = spending_response[tmp_channel+'spending'].min()
    x, y = get_point(minspend, maxspend, a, b, c, d)
    yfit1 = func1(spending_response[tmp_channel+'spending'], a, b, c, d)
    #ypredict = spending_response[tmp_channel+'spending'] * yfit1
    """
    p = func2(spending_response[tmp_channel+'spending'], spending_response['rate'], 7)
    return p

def main():
    optimal_spending = pd.read_csv(analysis_path + "5_200_2_reallocated_best_1.csv")
    hist_spending = hist_spending = pd.read_csv(analysis_path + "5_200_2_reallocated_hist.csv")

    mean_spend = metrics['mean_spend']
    print(mean_spend)

    #compare_optimal_distribution(optimal_spending)
    #compare_hist_distribution(hist_spending)
    """
    spending_threshold = {}
    for channel in colname:
        spending = draw_roas_rate(channel)
        spending_threshold[channel] = spending
    print(spending_threshold)
    """
    #channel_list = ['Apple_Search_Ads_iOS', 'Adwords_iOS', 'Facebook_Android']
    #channel_list = ['Snapchat_iOS', 'bytedanceglobal_int_Android','bytedanceglobal_int_iOS']
    channel_list = [ 'Tatari_TV', 'Facebook_iOS', 'Adwords_Android',]
    #channel_list = [ 'new_channels']
    #channel_list = [ 'Tatari_TV', 'new_channels']
    """
    for item in channel_list:
        print(item)
        draw_saturtion_customized([item])
    """
    #draw_saturtion_customized(colname)

    """
    item = 'Tatari_TV'
    mean_spending = metrics.loc[metrics['rn']==item, ['mean_spend']].values[0][0]
    get_margin_update('Tatari_TV', mean_spending, 1000)
    """

    #draw_roas_rate('Apple_Search_Ads_iOS')
    #draw_roas_rate('Adwords_iOS')
    #for item in colname:
    #    draw_roas_rate(item)

    small_channels = ['Snapchat_iOS', 'bytedanceglobal_int_Android','bytedanceglobal_int_iOS', 'Snapchat_Android']
    big_channels = ['Apple_Search_Ads_iOS', 'Adwords_iOS', 'Facebook_Android', 'Facebook_iOS', 'Tatari_TV', 'new_channels', 'Adwords_Android']
    #draw_saturtion_customized(small_channels)
    #draw_saturtion_customized(big_channels)
    draw_saturtion_customized(channel_list)
    """
    margins = defaultdict(list)
    for item in big_channels:
        mean_spending = metrics.loc[metrics['rn']==item, ['mean_spend']].values[0][0]
        for k in range(1, 11):
            tmp = 1000 + (k - 1) * 1000
            margins[item].append(get_margin_update(item, mean_spending, tmp))
    margins_df = pd.DataFrame.from_dict(margins)
    margins_df.to_csv(analysis_path + "big_margins_df_1.csv")
    """
    """
    for item in colname:
        draw_spending_response(item)
    """

if __name__ == '__main__':
    main()

