import numpy as np 
import pandas as pd 
import math
import matplotlib.pyplot as plt 
from sklearn import metrics 
import statsmodels.api as sm 
import pickle 
from collections import defaultdict
import seaborn as sns

# get parameters 
# calculation prediction of Bayesian sampling
# apply OLS to adjust parameters
# calculate prediction after adjustment
# calculate the R-squared
# calculate the ROAS

datafile_path = "/Users/yanchunyang/Documents/datafiles/pystan/"
L = 14  #carryover period
T = 7   #moving average period
Kb = 7
Km = 6
Kl = 391
media_list = ['AdWords', 'Apple Search Ads', 'Facebook','Snapchat', 'Tatari', 'bytedanceglobal_int']
sample_size = 1000
M = 330

def moving_average(a, n):
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n-1:] / n 

def expand_var_list(var_set, kv):
    var_list = []
    for item in var_set:
        for i in range(1, kv + 1):
            var_list.append(item + "." + str(i))
    return var_list

def expand_x_hill(Kl, Km):
    return ["x_hill." + str(s) + "." + str(i) for s in range(1, Kl + 1) for i in range(1, Km + 1)]

def get_parameter(df_parameter):
    var_list = ['sigma', 'ru']
    var_basic = ['beta_b']
    var_spending = ['beta_m', 'alpha', 'theta']

    var_list += expand_var_list(var_basic, Kb)
    var_list += expand_var_list(var_spending, Km)

    var_par = df_parameter.iloc[-sample_size:].loc[:, var_list].mean().values

    sigma = var_par[0]
    ru = var_par[1]
    beta_b = np.array(var_par[2:2 + Kb])
    beta_m = np.array(var_par[2 + Kb: 2 + Kb + Km])
    alpha = np.array(var_par[2 + Kb + Km:2 + Kb + Km* 2])
    theta = np.array(var_par[2 + Kb + Km * 2:2 + Kb + Km * 3])

    if len(var_list) != len(var_par):
        print("Parameters length can't match")
    return sigma, ru, beta_b, beta_m, alpha, theta
def get_ols_prediction(y, x_basic, x_hill_update):
    x_hill_mean = np.mean(x_hill_update, axis=0)
    x = np.concatenate([x_hill_mean, x_basic], axis=1)
    x_value = sm.add_constant(x)
    model = sm.OLS(y, x_value)
    results = model.fit()
    y_fitted = results.fittedvalues
    return y_fitted, results.rsquared, results

def get_bayesian_prediction(df_parameter):
    spending, basic, newuser, spending_origin = read_data()
    y_moving = moving_average(newuser[:-1], T)
    x_basic_moving = basic[T-1:-1,]
    sample = 1000
    beta_b_var = expand_var_list(['beta_b'], Kb)
    beta_m_var = expand_var_list(['beta_m'], Km)
    x_hill_var = expand_x_hill(Kl, Km)

    beta_b = df_parameter.iloc[-sample:].loc[:,beta_b_var].values
    beta_m = df_parameter.iloc[-sample:].loc[:, beta_m_var].values
    x_hill = df_parameter.iloc[-sample:].loc[:, x_hill_var].values
    x_hill_update = x_hill.reshape(sample, Kl, Km)
    x_m = np.sum(beta_m[:, np.newaxis, :] * x_hill_update, axis=2)
    x_b = np.dot(beta_b, x_basic_moving.T)
    sigma = df_parameter.iloc[-1000:].loc[:, ['sigma']].values.reshape(-1,1)
    ru = df_parameter.iloc[-1000:].loc[:, ['ru']].values.reshape(-1,1)
    prediction = sigma + ru + x_m + x_b
    prediction_mean = np.mean(prediction, axis=0)
    print(metrics.r2_score(prediction_mean, y_moving))
    #draw_graph_name(prediction_mean, y_moving, "prediction", "actual")

    y_fitted, rsquared, results = get_ols_prediction(y_moving, x_basic_moving, x_hill_update)
    print("rsquare from xhill")
    print(rsquared)
    print(results.summary())
    print(metrics.r2_score(y_fitted, y_moving))

def get_hill(alpha, theta, spending):
    coe = []
    for i in range(0, L):
        coe.append(L - i)
    data_length = spending.shape[0]
    media_length = spending.shape[1]
    x = np.zeros((data_length, media_length))
    for i in range(0, data_length):
      
        for k in range(0, media_length):
            coe_total = 0
            if i < L:
                for j in range(0, i + 1 ):
                    x[i][k] += spending[j][k] * alpha[k] ** (coe[L - i + j -1 ]-1-theta[k])**2
                    coe_total += alpha[k] ** (coe[L - i + j - 1 ]-1-theta[k])**2
            else:
                for j in range(0, L):
                    x[i][k] += spending[i- L + j + 1][k] * alpha[k] ** (coe[j]-1-theta[k])**2 
                    coe_total += alpha[k] ** (coe[j]-1-theta[k])**2 
            x[i][k] = math.log(x[i][k] / coe_total + 1)
    return x

def get_prediction_from_parameter(df_parameter):
    spending, basic, newuser, spending_origin = read_data()
    sigma, ru, beta_b, beta_m, alpha, theta = get_parameter(df_parameter)
    x_spending = get_hill(alpha, theta, spending[:-1])
    x_basic = basic[:-1,:]
    prediction = ru + sigma + np.matmul(np.array(beta_b), x_basic.T) + np.matmul(np.array(beta_m), x_spending.T)
    y = moving_average(newuser[:-1], 7)
    print(metrics.r2_score(prediction[T-1:], y))
    y_from_model = y_fitted_from_bayesian(df_parameter)
    print("y directly from model")
    print(np.square(np.corrcoef(y_from_model, y)))
    print(np.square(np.corrcoef(y_from_model[0:M], y[0:M])))
    print(np.square(np.corrcoef(y_from_model[M:], y[M:])))
    # compare with OLS
    x_combine = np.concatenate([x_spending, x_basic], axis=1)
    x_value = sm.add_constant(x_combine[T-1:, :])
    model = sm.OLS(y[0:M], x_value[0:M,:])
    results = model.fit()
    y_fitted = results.fittedvalues
    y_predicted = results.predict(x_value[M:,:])
    print("r-squared from parameter")
    print(results.rsquared)
    print(results.summary())
    print("r squared from metrics.r2_square")
    print(metrics.r2_score(y_fitted, y[0:M]))
    print("adjusted R squared is")
    print(np.square(np.corrcoef(y_fitted, y[0:M])))
    print("adjusted R squared for prediction is")
    print(np.square(np.corrcoef(y_predicted, y[M:])))