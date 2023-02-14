import io
import os
from pickletools import optimize 
import re 
import shutil
import tensorflow as tf 
from tensorflow.keras import Sequential
from tensorflow.keras import layers
from tensorflow.keras.layers import Dense, Embedding, GlobalAveragePooling1D
from random import sample 
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
from udf import *
import pickle

datafile_path = "/Users/yanchunyang/Documents/datafiles/ltv_ml/"
vocab_size = 1083
num_ns = 4

def read_data():
    df = pd.read_csv(datafile_path + "event_history.csv")
    event_type = pd.read_csv(datafile_path + "event_type.csv", header=0)
    df = df.dropna() 
    df['userid'] = df['USER_ID'].astype(int)
    dfuser = df.groupby(['userid', 'DATETIME']).agg({'EVENT_STR': ','.join}).reset_index() 
    dfuser['length'] = dfuser['EVENT_STR'].apply(lambda x: len(x.strip().split(",")))
    df_filter = dfuser.loc[dfuser['length']>50, :]
    return event_type, df_filter 

def output_dic(vocab, inverse_vocab):
    with open(datafile_path + "vocab.pk", 'wb') as f:
        pickle.dump(vocab, f)

    with open(datafile_path + "inverse.pk, 'wb") as f:
        pickle.dump(inverse_vocab, f)

def clean_data(event_type, df):
    # get word dictionary
    event_words = list(event_type['EVENT_TYPE'])
    vocab, index = {}, 1
    vocab['pad'] = 0
    for token in event_words:
        vocab[token] = index 
        index += 1 

    inverse_vocab = {index: token for token, index in vocab.items()}
    eventstr_seq = list(df['EVENT_STR'])
    eventstr_sample = sample(eventstr_seq, 10000)
    training_token = [get_token(x, vocab) for x in eventstr_sample]
    output_dic(vocab, inverse_vocab)
    return training_token

def generate_training_data(sequences, window_size, num_ns, vocab_size, seed):
    targets, contexts, labels = [], [], []

    sampling_table = tf.keras.preprocessing.sequence.make_sampling_table(vocab_size)

    for sequence in sequences:
        positive_skip_grams, _ = tf.keras.preprocessing.sequence.skipgrams(
            sequence,
            vocabulary_size=vocab_size,
            sampling_table=sampling_table,
            window_size=window_size,
            negative_samples=0
        )
        for target_word, context_word in positive_skip_grams:
            context_class = tf.expand_dims( tf.constant([context_word], dtype="int64"), 1)
            negative_sampling_candidate, _, _ = tf.random.log_uniform_candidate_sampler(
                true_classes=context_class,
                num_true=1,
                num_sampled=num_ns,
                unique=True,
                range_max=vocab_size,
                seed=seed,
                name="negative_sampling"
            )
            negative_sampling_candidates = tf.expand_dims(negative_sampling_candidate, 1)
            context = tf.concat([context_class, negative_sampling_candidates], 0)
            label = tf.constant([1] + [0]*num_ns, dtype="int64")

            targets.append(target_word)
            contexts.append(context)
            labels.append(label)

    return targets, contexts, labels 

class Event2Vec(tf.keras.Model):
    def __init__(self, vocab_size, embedding_dim):
        super(Event2Vec, self).__init__()
        self.target_embedding = layers.Embedding(vocab_size, embedding_dim, input_length=1, name="e2v_embedding")
        self.context_embedding = layers.Embedding(vocab_size, embedding_dim, input_length=num_ns+1)

    def call(self, pair):
        target, context = pair
        if len(target.shape) == 2:
            target = tf.squeeze(target, axis=1)
        word_emb = self.target_embedding(target)
        context_emb = self.context_embedding(context)
        dots = tf.einsum('be,bce->bc', word_emb, context_emb)
        return dots 

def training_model(targets, contexts, labels):
    targets = np.array(targets)
    contexts = np.array(contexts)[:,:,0]
    labels = np.array(labels)

    AUTOTUNE = tf.data.AUTOTUNE
    BATCH_SIZE = 1024
    BUFFER_SIZE = 10000
    dataset = tf.data.Dataset.from_tensor_slices(((targets, contexts), labels))
    dataset = dataset.shuffle(BUFFER_SIZE).batch(BATCH_SIZE, drop_remainder=True)
    dataset = dataset.cache().prefetch(buffer_size=AUTOTUNE)

    embedding_dim = 24 
    event2vec = Event2Vec(vocab_size, embedding_dim)
    event2vec.compile(optimizer='adam', loss=tf.keras.losses.CategoricalCrossentropy(from_logits=True), metrics=['accuracy'])
    tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir="logs")

    event2vec.fit(dataset, epochs=20, callbacks=[tensorboard_callback])
    weights = event2vec.get_layer('e2v_embedding').get_weights()[0]

    np.savetxt(datafile_path + "weights_ide.csv", weights, delimiter=",")
    print("The training ends")

def main():
    event_type, df_filter = read_data()
    sequences = clean_data(event_type, df_filter)
    targets, contexts, labels = generate_training_data(sequences, window_size=6, num_ns=4, vocab_size=1083, seed=123)
    training_model(targets, contexts, labels)

if __name__ == '__main__':
    main()