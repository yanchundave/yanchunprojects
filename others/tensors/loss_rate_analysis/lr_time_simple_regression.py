import datetime
import tensorflow as tf
import numpy as np
import logging
import os
from tensorflow.keras import activations
from tensorflow.keras import callbacks
from tensorflow.keras import layers
from tensorflow.keras import models

from tensorflow import feature_column as fc
import pandas as pd
import matplotlib.pyplot as plt


path = "/Users/yanchunyang/Documents/training/learning_datasets/"
file1_name = "lossrate_disbursement.csv"
file2_name = "loss_arrival.csv"

def read_data():
    df_features = pd.read_csv(path + file1_name)
    df_loss = pd.read_csv(path + file2_name)
    return df_features, df_loss


def preprocessing(df_loss, df_features):
    df_loss['timestamp'] = pd.to_datetime(df_loss['DISBURSEMENT_DS'])
    df_loss['disbursement'] = (df_loss['TOTAL_DISBURSEMENT_AMOUNT'] -  \
                           np.min(df_loss['TOTAL_DISBURSEMENT_AMOUNT']))/(np.max(df_loss['TOTAL_DISBURSEMENT_AMOUNT']) - \
                                                                          np.min(df_loss['TOTAL_DISBURSEMENT_AMOUNT']))
    loss_columns = ['timestamp', 'disbursement', 'D_0_LOSS_RATE', 'D_1_LOSS_RATE']
    df_loss_update = df_loss.loc[:, loss_columns].sort_values(by=['timestamp'])

    df_features['timestamp'] = pd.to_datetime(df_features['DISBURSEMENT_DS'])
    df_features['prior_info'] = 1

    columns = ['timestamp', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'prior_info']
    df_features_update = df_features.loc[:, columns].sort_values(by=['timestamp'])

    df_combine = pd.merge(df_features_update, df_loss_update, on=['timestamp'], how='inner')

    return df_combine[20:-20].reset_index()

def get_features_label(df, n_loss = 14):
    features = []
    linear_feature_columns = [ '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'prior_info', 'disbursement']
    for i in range(n_loss, df.shape[0]):
        features.append(df['D_0_LOSS_RATE'][i - n_loss: i])
    linear_features = np.array(df.loc[n_loss:, linear_feature_columns])
    time_features = np.array(features)
    label = np.array(df['D_0_LOSS_RATE'][n_loss:] )
    df_combine = np.column_stack([linear_features, time_features])
    return df_combine, label


def get_dataset(linear_features, label):
    length = linear_features.shape[0]
    train_linear, train_label = linear_features[0:int(0.8*length)], label[0:int(0.8*length)]
    test_linear, test_label = linear_features[int(0.8*length):],  label[int(0.8*length):]
    return train_linear,test_linear, train_label,  test_label

def tnn_linear_model(n):
    input_dense = tf.keras.Input(shape=(n, ), name='input_dense')
    dense_output = layers.Dense(10, activation='relu')(input_dense)
    output = layers.Dense(1, activation='sigmoid')(dense_output)
    model = models.Model(inputs=input_dense, outputs=output)
    model.compile(loss='mse', optimizer='adam', metrics=['mse'])
    return model

def get_test(model, test_feature, test_label):
    results = []
    length = test_feature.shape[0]
    prediction = model.predict(test_feature[0].reshape(1,-1))
    results.append(prediction[0][0])
    for i in range(1, length):
        tmp = test_feature[i].reshape(1, -1)
        tmp[0][-1] = results[-1]
        prediction = model.predict(tmp)
        results.append(prediction[0][0])
    return np.array(results)


