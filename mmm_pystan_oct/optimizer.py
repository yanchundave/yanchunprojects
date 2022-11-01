# This file need run under conda activate cvxpy, not under stan
import cvxpy as cp
import pickle
import math 
import numpy as np

datafile_path = "/Users/yanchunyang/Documents/datafiles/"
L = 14
N = 6
media_list = ['AdWords', 'Apple Search Ads', 'Facebook','Snapchat', 'Tatari', 'bytedanceglobal_int']

def obtain_parameters():
    with open(datafile_path + "opt_df_spending.p", 'rb') as f:
        df_spending = pickle.load(f)

    with open(datafile_path + "opt_beta.p", "rb") as f:
        beta = pickle.load(f)
    # alpha
    with open(datafile_path + "opt_alpha.p", "rb") as f:
        alpha = pickle.load(f)   
    # theta
    with open(datafile_path + "opt_theta.p", "rb") as f:
        theta = pickle.load(f)
    
    return df_spending, beta, alpha, theta

def get_min_max(df):
    min_list = []
    max_list = []
    for i in range(0, len(media_list)):
        min_list.append(df[media_list[i]].min())
        max_list.append(df[media_list[i]].max())

    return min_list, max_list

def get_coes(alpha, theta, max_list):
    coes = []
    for i in range(0, 6):
        tmp = 0
        for j in range(0, 15):
            d = math.pow(j - theta[i], 2)
            tmp += math.pow(alpha[i], d)
        coes.append(tmp / max_list[i])
    return coes 



def get_coes_update(alpha, theta, max_list):
    coes = []
    for i in range(0, 6):
        tmp = 0
        for j in range(0, 15):
            d = math.pow(j - theta[i], 2)
            tmp += math.pow(alpha[i], d)
        coes.append(tmp / max_list[i])
    return coes

def opt_solver():  
    df_spending, beta, alpha, theta = obtain_parameters()
    min_list, max_list = get_min_max(df_spending)
    coes = get_coes(alpha, theta, max_list)
    beta_m = beta[1:7]

    x = cp.Variable(N)
    #obj = cp.Maximize(cp.sum(beta_m * cp.log(coes * x + 1)))
    obj = cp.Maximize(cp.sum(beta_m * cp.log((x -min_list) / max_list + 1)))
    constraints = [x >= min_list, sum(x) <= 200000]

    prob = cp.Problem(obj, constraints)
    print("Optimal value", prob.solve())
    print(x.value)

opt_solver()