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

    return df_combine.iloc[20:].reset_index()

def get_timeseries(dfs, n_loss=30):
    features = []
    linear_feature_columns = [ '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'prior_info', 'disbursement']
    for i in range(n_loss, dfs.shape[0]):
        features.append(dfs['D_0_LOSS_RATE'][i-n_loss:i])
    linear_features_raw = np.array(dfs.loc[n_loss:,linear_feature_columns])
    linear_features = np.expand_dims(linear_features_raw, axis=2)
    time_features_raw = np.array(features)
    time_features = np.expand_dims(time_features_raw, axis=2)
    label = np.array(dfs['D_0_LOSS_RATE'][n_loss:])
    print(linear_features.shape)
    print(time_features.shape)
    print(label.shape)
    return linear_features.astype(np.float32), time_features.astype(np.float32), label.astype(np.float32)


def get_dataset(linear_features, time_features, label):
    length = linear_features.shape[0]
    train_linear, train_time, train_label = linear_features[0:int(0.6*length)], time_features[0:int(0.6*length)], label[0:int(0.6*length)]
    valid_linear, valid_time, valid_label = linear_features[int(0.6*length): int(0.8*length)], time_features[int(0.6*length): int(0.8*length)], label[int(0.6*length): int(0.8*length)]
    test_linear, test_time, test_label = linear_features[int(0.8*length):], time_features[int(0.8*length):], label[int(0.8*length):]
    train_dataset = tf.data.Dataset.from_tensor_slices(({'input_dense':train_linear, 'input_rnn':train_time}, train_label))
    val_dataset = tf.data.Dataset.from_tensor_slices(({'input_dense':valid_linear, 'input_rnn':valid_time}, valid_label))
    test_dataset = tf.data.Dataset.from_tensor_slices(({'input_dense':test_linear, 'input_rnn':test_time}, test_label))
    batch_size = 8
    train_dataset = train_dataset.batch(batch_size)
    val_dataset = val_dataset.batch(batch_size)
    test_dataset = test_dataset.batch(batch_size)
    return train_dataset, val_dataset, test_dataset


def get_model():
    input_dense = tf.keras.Input(shape=(12 ,1), name='input_dense')
    input_rnn = tf.keras.Input(shape=( 10, 1), name='input_rnn')
    conv_output = layers.Conv1D(filters=4,
                           kernel_size=(3,),
                           activation='relu')(input_dense)
    dense_output = layers.Dense(6, activation='relu')(conv_output)
    dense_output = layers.Flatten()(dense_output)
    rnn_output = layers.LSTM(10, return_sequences=True, input_shape=[None, 1])(input_rnn)
    rnn_output = layers.LSTM(4, return_sequences=False)(rnn_output)
    concatenated = layers.Concatenate()([dense_output, rnn_output])
    output = layers.Dense(1, activation='sigmoid')(concatenated)
    model = models.Model(inputs=(input_dense, input_rnn), outputs=output)
    model.compile(loss=tf.keras.losses.MeanSquaredError(), optimizer='adam', metrics=['mse'])
    return model
