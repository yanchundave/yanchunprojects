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


def preprocessing(df_features, df_loss):

    df_combine = pd.merge(df_features, df_loss, on=['timestamp'], how='inner')
    df1 = df_combine.iloc[20:]
    df1 = df1.reset_index()
    return df1

def get_timeseries(df):
    features = []
    linear_feature_columns = [ '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'prior_info', 'disbursement']
    for i in range(30, df.shape[0]):
        features.append(df3['D_0_LOSS_RATE'][i-30:i])
    linear_features = np.array(df.loc[30:,linear_feature_columns])
    time_features_raw = np.array(features)
    time_features = np.expand_dims(time_features_raw, axis=2)
    label = np.array(df3['D_0_LOSS_RATE'][30:])
    print(linear_features.shape)
    print(time_features.shape)
    print(label.shape)
    return linear_features.astype(np.float32), time_features.astype(np.float32), label.astype(np.float32)

def get_model():
    input_dense = tf.keras.Input(shape=(12 ,), name='input_dense')
    input_rnn = tf.keras.Input(shape=( 30, 1), name='input_rnn')
    dense_output = layers.Dense(30, activation='relu')(input_dense)
    dense_output = layers.Dense(10, activation='relu')(dense_output)
    rnn_output = layers.LSTM(10, return_sequences=True, input_shape=[None, 1])(input_rnn)
    rnn_output = layers.LSTM(5, return_sequences=False)(rnn_output)
    concatenated = layers.Concatenate()([dense_output, rnn_output])
    output = layers.Dense(1, activation='sigmoid')(concatenated)
    model = models.Model(inputs=(input_dense, input_rnn), outputs=output)
    model.compile(loss='mse', optimizer='adam', metrics=['mse'])
    return model

def train():
    early_stopping = tf.keras.callbacks.EarlyStopping(monitor='val_loss',
                                                        patience=2,
                                                        mode='min')
    model = get_model()
    df_features, df_loss = read_data()
    df = preprocessing(df_features, df_loss)
    train_linear, train_time, train_label = get_timeseries(df)
    history_np = model.fit({'input_dense': train_linear, 'input_rnn': train_time}, train_label,
                        epochs=10, batch_size=32, validation_split=0.2)




