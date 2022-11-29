# Check if the generated data is the sampled parameter's result. The experiment result is  no.
import numpy as np
import pandas as pd
import pickle
import matplotlib.pyplot as plt
from global_variable import *

def read_dataset():
    with open(datafile_path + "spending.p", "rb") as f:
        df_spending = pickle.load(f)

    with open(datafile_path + "basic.p", "rb") as f:
        df_basic = pickle.load(f)

    with open(datafile_path + "newuser.p", "rb") as f:
        y = pickle.load(f)

    print("spending shape is %2d, %2d" % (df_spending.shape[0], df_spending.shape[1]))
    print("basic shape is %2d, %2d" % (df_basic.shape[0], df_basic.shape[1]))
    print("y value shape is %2d" % (len(y)))

    #return df_spending, df_basic, y

def read_csv():
    df = pd.read_csv(datafile_path + "platform_raw.csv")
    print(df.columns)

def read_parameters():
    with open(datafile_path + "pystan_latest_train.p", "rb") as f:
        df_parameter = pickle.load(f)
    return df_parameter

def draw_graph_name(a, b, a_name, b_name):
    plt.plot(a, label=a_name)
    plt.plot(b, label=b_name)
    plt.legend()
    plt.show()

def get_x_hill():
    df_spending, df_basic, y = read_dataset()
    df_parameter = read_parameters()
    sigma = df_parameter['sigma'].values
    ru = df_parameter['ru'].values

    beta_b = ['beta_b.1', 'beta_b.2', 'beta_b.3', 'beta_b.4', 'beta_b.5', 'beta_b.6', 'beta_b.7']
    beta_m = ['beta_m.1', 'beta_m.2', 'beta_m.3', 'beta_m.4', 'beta_m.5', 'beta_m.6']

    x_hill_columns = ["x_hill.391." + str(i) for i in range(1, Km + 1)]
    beta_b_values = df_parameter.loc[:, beta_b].values
    beta_m_values = df_parameter.loc[:, beta_m].values
    x_hill_value = df_parameter.loc[:, x_hill_columns].values

    x_basic_sample = df_basic[-2]

    t1 = np.dot(beta_b_values , x_basic_sample.T)
    t2 = np.sum(np.multiply(beta_m_values, x_hill_value), axis= 1)

    y_fitted = t1 + t2 + sigma + ru

    print(y_fitted[-20:])
    print(df_parameter['y_fitted.391'].values[-20:])

#get_x_hill()

#read_dataset()

read_csv()