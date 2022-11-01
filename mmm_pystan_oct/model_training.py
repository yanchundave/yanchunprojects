import stan
import numpy as np
import pandas as pd
import pickle
import nest_asyncio
import time
from global_variable import *

nest_asyncio.apply()


"""
Step 3: This is model training code snippet. It is training part.

The input data are scaled spending data for each channel and new daily PV users.

"""

L = 14  #carryover period
T = 7   #moving average period
Kb = 7 #basic variable

def read_dataset():

    '''
    Returns dataframe of scaled channel spending, basic and daily new user acquared.
    '''

    with open(datafile_path + "spending.p", "rb") as f:
        df_spending = pickle.load(f)
    with open(datafile_path + "basic.p", "rb") as f:
        df_basic = pickle.load(f)
    with open(datafile_path + "newuser.p", "rb") as f:
        y = pickle.load(f)
    with open("spending_column.txt", 'r') as f:
        spending_str = f.readline()
    return df_spending, df_basic, y, spending_str


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


def adjust_data(df_spending, df_basic, y):
    '''
    Returns the datasets for training after moving average.

            Parameters:
                    df_spending (dataframe): spending data after scaling
                    df_basic (dataframe): basic variable data after scaling
                    y (list): daily new user acquired after scaling
            Returns:
                    a list for training with spending, basic and y value
    '''

    datasets = []
    data_length = df_spending.shape[0]
    x_spending_moving = df_spending[T-1: data_length - 1, :] #Remove the last point since the data is incomplete
    x_basic_moving = df_basic[T-1: data_length -1, :]
    y_moving = moving_average(y[:-1], T)

    datasets.append(x_spending_moving)
    datasets.append(x_basic_moving)
    datasets.append(y_moving)
    with open(datafile_path + 'datasets.p', "wb") as f:  #Pickle the middle dataset for conviniently hyperparameters searching
        pickle.dump(datasets, f)

    return datasets


def save_parameters(Km, Kl):
    parameters = {'L': L, 'T': T, 'Kb': Kb, 'Km': Km, 'Kl': Kl}
    with open("parameter.p", "wb") as f:
        pickle.dump(parameters, f)


def train_model(model_file):
    '''
    Return the trained model posterier result.

            Parameters:
                    model_file (string): pystan model file
            Returns:
                    fit (model result): trained model result
                    df (dataframe): posterior distribution of variables
    '''

    print("start")
    print(time.strftime("%H:%M:%S", time.localtime()))
    df_spending, df_basic, y, spending_str= read_dataset()
    datasets = adjust_data(df_spending, df_basic, y)
    x_spending, x_basic, y_moving = datasets[0], datasets[1], datasets[2]

    Km = len(spending_str[:-1].strip().split(",")) #platform information
    Kl = df_spending.shape[0] - T   #total data points - T
    M = Kl

    y_moving_train = y_moving[0:M]
    with open(model_file, 'r') as f:
        model_description = f.read()

    stan_data = {
        "L": L,
        "M" : Kl,
        "N": Kl,
        "Kb": T,
        "Km": Km,
        "x_b": x_basic,
        "x_m": x_spending,
        "y": y_moving_train

    }

    # save the parameter for analysis part
    save_parameters(Km, Kl)

    posterior = stan.build(model_description, data=stan_data, random_seed=1)
    fit = posterior.sample(num_chains=4, num_samples=2000)
    df = fit.to_frame()
    print("The end time is ")
    print(time.strftime("%H:%M:%S", time.localtime()))
    return fit, df


def main():

    '''
    Main file to read the training data.
    Train the model
    Dump the result to pickle files for result analysis.
    '''

    model_file = "./stan_model.txt"
    fit, df = train_model(model_file)

    with open(datafile_path + "pystan_fit_train.p", "wb") as f:
        pickle.dump(fit, f)
    with open(datafile_path + "pystan_latest_train.p", "wb") as f:
        pickle.dump(df, f)

    print("The training is completed")


if __name__ == '__main__':
    main()