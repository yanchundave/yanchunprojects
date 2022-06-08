"""This script is a simple RNN model.
It is used to calibrate the R-squared of MMM.
The input datasets are the scaled datasets from MMM model 
Download this script and run after the MMM
Need install tensorflow
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from keras import Sequential
from keras.layers import Dense, LSTM, Dropout
import pickle

Km = 6
Kb = 7
L = 14
T = 7

class RnnMmm:
    
    def __init__(self, spending,newuser, datafile):
        self.spending = spending
        self.newuser = newuser
        self.datafile = datafile

    @classmethod
    def read_data(cls, datafile_path):
        filenames = ['spending', 'newuser']
        x = []
        for item in filenames:
            with open(datafile_path + item + "_pickle.p", 'rb') as f:
                x.append(pickle.load(f))
        spending, newuser = x[0], x[1]
        return cls(spending, newuser, datafile_path)

    def get_timeseries(self, df_x, l):
        cols, names = list(), list()
        varname = df_x.shape[1]
        for i in range(l, 0, -1):
            cols.append(df_x.shift(i))
            names += [('var%d(t-%d)' % (j+1, i)) for j in range(varname)]
        cols.append(df_x)
        names += [('var%d(t-%d)' % (j+1, 0)) for j in range(varname)]
        return cols, names

    def data_preparation(self):
        spending_columns = [ "var_" + str(i) for i in range(1, Km+1)]
        df_spending = pd.DataFrame(self.spending, columns=spending_columns)
        df_y = self.newuser
        ml = L
        cols, names = self.get_timeseries(df_spending, ml)
        agg = pd.concat(cols, axis=1)
        agg.columns = names 
        x = agg.iloc[L+1:, :].values
        y_moving = self.moving_average(df_y, T)
        y = y_moving[L-T+2:] 
        sample_size = x.shape[0]
        var_size = Km
        train_x = x.reshape(sample_size, L+1, var_size)
        print(train_x.shape)
        print(len(y))
        self.model_training(train_x, y)
        
    def model_training(self, x, y):
        model = Sequential()
        model.add(LSTM(units=40, return_sequences=True, input_shape=(x.shape[1], x.shape[2])))
        model.add(LSTM(units=20))
        model.add(Dense(units=60))
        model.add(Dense(units=40))
        model.add(Dense(units=20))
        model.add(Dense(units=1))
        model.compile(optimizer='adam', loss='mean_squared_error')
        model.fit(x, y, epochs=200, batch_size=75)
        predicted_value =  model.predict(x)
        prediction = predicted_value[:, 0]
        self.model_analysis(prediction, y)

    def model_analysis(self, prediction, actual):
        r2 = np.square(np.corrcoef(prediction, actual))
        print("r2 is " + str(r2))
        plt.subplot(1,1,1)
        plt.plot(prediction, color='red')
        plt.plot(actual, color='green')
        plt.savefig(self.datafile + "pystan/rnn_compare.png")

    def moving_average(self, a, n):
        ret = np.cumsum(a, dtype=float)
        ret[n:] = ret[n:] - ret[:-n]
        return ret[n-1:] / n 


def main():
    data_file = "/Users/yanchunyang/Documents/datafiles/"
    model = RnnMmm.read_data(data_file)
    model.data_preparation()

if __name__ == '__main__':
    main()