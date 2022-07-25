import io
import os
import re
import shutil
import string
import tensorflow as tf
from tensorflow.keras import Sequential, layers
from tensorflow.keras.layers import Dense, Embedding, GlobalAveragePooling1D
from tensorflow.keras.layers import TextVectorization
from tensorflow.keras.layers import Dense
from keras import Sequential
from udf import read_from_database
import pandas as pd
from global_variable import *
import pickle
from random import sample
import numpy as np
import math

SEED=2
NUM_NS=4

def read_data():
    sql_str = """
    SELECT * FROM DBT.DEV_YANCHUN_PUBLIC.EVENT_SEQ
    """
    result = read_from_database(sql_str)
    df = pd.DataFrame(result, columns=['front_event', 'middle_event', 'back_event', 'userstr'])
    df.to_csv(datafile_path + "event_seq.csv")

    print(df.head(4))

def generate_dic():
    df = pd.read_csv(datafile_path + "event_seq.csv", header=0)
    words = []
    words_seq = df['userstr']
    for item in words_seq:
        splits = item.strip().split(",")
        words += splits

    customers = list(set(words))
    vocab, index = {}, 1
    vocab['0'] = 0
    for token in customers:
        vocab[token] = index
        index += 1 
    inverse_vocab = {index: token for token, index in vocab.items()}
    return vocab, inverse_vocab, list(words_seq)

def generate_training_data(sequences, window_size, num_ns, vocab_size, seed):
    targets, contexts, labels = [], [], []
    
    sampling_table = tf.keras.preprocessing.sequence.make_sampling_table(vocab_size)
    
    for sequence in sequences:
        positive_skip_grams, _ = tf.keras.preprocessing.sequence.skipgrams(
        sequence,
        vocabulary_size=vocab_size,
        sampling_table=sampling_table,
        window_size=window_size,
        negative_samples=0)
        
        for target_word, context_word in positive_skip_grams:
            context_class = tf.expand_dims(
            tf.constant([context_word], dtype="int64"), 1)
            negative_sampling_candidate, _, _ = tf.random.log_uniform_candidate_sampler(
                true_classes=context_class,
                num_true=1,
                num_sampled=num_ns,
                unique=True,
                range_max=vocab_size,
                seed=SEED,
                name="negative_sampling"
            )
            negative_sampling_candidates = tf.expand_dims(negative_sampling_candidate, 1)
            context = tf.concat([context_class, negative_sampling_candidates], 0)
            label = tf.constant([1] + [0]*num_ns, dtype="int64")
            
            targets.append(target_word)
            contexts.append(context)
            labels.append(label)
        
    return targets, contexts, labels

class Word2Vec(tf.keras.Model):
  def __init__(self, vocab_size, embedding_dim):
    super(Word2Vec, self).__init__()
    self.target_embedding = layers.Embedding(vocab_size,
                                      embedding_dim,
                                      input_length=1,
                                      name="w2v_embedding")
    self.context_embedding = layers.Embedding(vocab_size,
                                       embedding_dim,
                                       input_length=NUM_NS+1)

  def call(self, pair):
    target, context = pair
    # target: (batch, dummy?)  # The dummy axis doesn't exist in TF2.7+
    # context: (batch, context)
    if len(target.shape) == 2:
      target = tf.squeeze(target, axis=1)
    # target: (batch,)
    word_emb = self.target_embedding(target)
    # word_emb: (batch, embed)
    context_emb = self.context_embedding(context)
    # context_emb: (batch, context, embed)
    dots = tf.einsum('be,bce->bc', word_emb, context_emb)
    # dots: (batch, context)
    return dots


def train_model(vocab, inverse_vocab, df_str):
    word_token = []
    max_length = 0
    for item in df_str:
        splits = item.strip().split(",")
        max_length = max(max_length, len(splits))
        if len(splits) <= 500:
            word_token.append([vocab[x] for x in splits])
    print("word_token length is " + str(len(word_token)))
    print("Longest string is " + str(max_length))
    vocab_size = len(vocab.keys())
    targets, contexts, labels = generate_training_data(word_token, window_size=2, num_ns=NUM_NS, vocab_size=vocab_size, seed=SEED)
    targets = np.array(targets)
    contexts = np.array(contexts)[:,:,0]
    labels = np.array(labels)
    BATCH_SIZE = 1024
    BUFFER_SIZE = 10000
    print("running here")
    dataset = tf.data.Dataset.from_tensor_slices(((targets, contexts), labels))
    dataset = dataset.shuffle(BUFFER_SIZE).batch(BATCH_SIZE, drop_remainder=True)
    embedding_dim = 3
    word2vec = Word2Vec(vocab_size, embedding_dim)
    word2vec.compile(optimizer='adam',
                 loss=tf.keras.losses.CategoricalCrossentropy(from_logits=True),
                 metrics=['accuracy'])  
    tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir="logs")
    word2vec.fit(dataset, epochs=20, callbacks=[tensorboard_callback])
    weights = word2vec.get_layer('w2v_embedding').get_weights()[0]
    print(weights.shape)
    return weights


def main(): 
    #read_data()
    
    vocab, inverse_vocab, df_str = generate_dic()
    """
    with open(datafile_path + "vocab.pk", 'wb') as f:
        pickle.dump(vocab, f)

    with open(datafile_path + "inverse_vocab", 'wb') as f:
        pickle.dump(inverse_vocab, f)

    print(len(vocab.keys()))
    """
    weights = train_model(vocab, inverse_vocab, df_str)
    with open(datafile_path + "weight.pk", 'wb') as f:
        pickle.dump(weights, f)
    
    print("training end")

if __name__ == '__main__':
    main()


