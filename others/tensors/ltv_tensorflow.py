import datetime
import logging
import os
import numpy as np
import tensorflow as tf
from tensorflow.keras import activations
from tensorflow.keras import callbacks
from tensorflow.keras import layers
from tensorflow.keras import models

from tensorflow import feature_column as fc
import pandas as pd

df = pd.read_csv("~/Downloads/df_data_test.csv")
categorical_columns = ['PLATFORM', 'ATTRIBUTION', 'NETWORK','BANK_CATEGORY','FIRST_TRANS', 'FORECAST_DATE']
numerical_columns = ['BOD_ACCOUNT_OPEN_USER', 'BOD_DIRECT_DEPOSIT_USER',
                     'HAS_VALID_CREDENTIALS', 'APPROVED_BANK_COUNT',
                     'FREQUENCY', 'RECENCY', 'T', 'MONETARY',
                     'MONETARY_TOTAL', 'MAX_REV',
                     'MAX_PRINCIPLE', 'SETTLEMENT_DATE_DEV',
                     'DISBURSE_DATE_DEV', 'NET_REV_DEV', 'SLOPE', 'INTERCEPT',
                     'SETTLED_RATE', 'OUTSTANDING_AMOUNT','PRIOR_REVENUE']

label_column = 'SUMREVENUE'

dfupdate = df.loc[:, categorical_columns + numerical_columns + [label_column]]

dfupdate.to_csv("~/Downloads/dfupdate.csv")
patterns = "/Users/yanchunyang/Downloads/dfupdate.csv"

columns = list(dfupdate.columns)
columnsupdate = ['index'] + columns

def features_and_labels(row_data):
    label = row_data.pop(label_column)
    return row_data,label

def load_dataset(pattern, batch_size, num_repeat):

    dataset = tf.data.experimental.make_csv_dataset(
        file_pattern=pattern,
        batch_size=batch_size,
        column_names=columnsupdate,
        num_epochs = num_repeat,
    )

    dataset = dataset.map(map_func=features_and_labels)
    return dataset

def create_train_dataset(pattern, batch_size):
    dataset = load_dataset(pattern, batch_size, num_repeat=None)
    return dataset.prefetch(1)

train_path = "/Users/yanchunyang/Downloads/dfupdate.csv"
trainds= create_train_dataset(train_path, 32)
trainds= create_train_dataset(train_path, 32)

num_tokens = {}
values = [3, 4, 107, 4]
for col in categorical_columns[0:-2]:
    num_tokens[col] = values[0]
    values.pop(0)

vocab_list = {}
for col in categorical_columns[:-2]:
    vocab_list[col] = list(dfupdate[col].unique())


inputs = {
            colname: layers.Input(name=colname, shape=(1,), dtype='float32')
              for colname in numerical_columns
            }

inputs.update({
    colname: layers.Input(name=colname, shape=(1,), dtype='string') for colname in categorical_columns})



def get_category_encoding_layer(name, vocab, num_token):
  # Create a layer that turns strings into integer indices.

    index = layers.StringLookup(vocabulary=vocab)


    encoder = layers.CategoryEncoding(num_tokens=num_token)


    return lambda feature: encoder(index(feature))

def transform(inputs):
    outputs = {}
    encoded_feature=[]
    for col in numerical_columns:
        outputs[col] = inputs[col]
        encoded_feature.append(outputs[col])
    for col in ['FIRST_TRANS', 'FORECAST_DATE']:
        outputs[col] = layers.Lambda(lambda x: tf.strings.to_number(tf.strings.substr(x, 5, 2)), output_shape=(1,))(inputs[col])
        encoded_feature.append(outputs[col])
    for col in ['PLATFORM', 'ATTRIBUTION', 'NETWORK','BANK_CATEGORY']:

        outputs[col] = get_category_encoding_layer(col, vocab_list[col], num_tokens[col])(inputs[col])

        encoded_feature.append(outputs[col])

    outputs_update = tf.keras.layers.concatenate(encoded_feature)


    return outputs_update

lr_optimizer = tf.keras.optimizers.Adam(learning_rate=0.1)

output_dir = "/Users/yanchunyang/Downloads/"
checkpoint_path = os.path.join(output_dir, 'checkpoints.weights.h5')
tensorboard_path = os.path.join(output_dir, 'tensorboard')
tensorboard_cb = callbacks.TensorBoard(tensorboard_path)

def rmse(y_true, y_pred):
    return tf.sqrt(tf.reduce_mean(tf.square(y_pred - y_true)))

x = transform(inputs)

nnsize = [32, 8]
for layer, nodes in enumerate(nnsize):
    x = layers.Dense(nodes, activation='relu', name='h{}'.format(layer))(inputs=x)
output = layers.Dense(1, name='revenue')(x)
checkpoint_cb = callbacks.ModelCheckpoint(
        checkpoint_path,
        save_weights_only=True,
        verbose=1
    )
model = tf.keras.Model(inputs, output)

model.compile(optimizer=lr_optimizer, loss='mse', metrics=[rmse, 'mse'])

history = model.fit(
    trainds,
    epochs=10,
    callbacks=[checkpoint_cb, tensorboard_cb]
)



