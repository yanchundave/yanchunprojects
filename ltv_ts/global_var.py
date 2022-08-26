import os
LOAD = False         # True = load previously saved model from disk?  False = (re)train the model
SAVE = "/_NBEATS_model_03.pth.tar"   # file name to save the model under

EPOCHS = 10
INLEN =  30         # input size
BLOCKS = 4
LWIDTH = 30
BATCH = 64          # batch size
LEARN = 1e-3        # learning rate
VALWAIT = 1         # epochs to wait before evaluating the loss on the test/validation set
N_FC = 1            # output size

RAND = 42           # random seed
N_SAMPLES = 50     # number of times a prediction is sampled from a probabilistic model
N_JOBS = -1         # parallel processors to use;  -1 = all processors

# default quantiles for QuantileRegression
QUANTILES = [0.01,  0.2, 0.5, 0.8, 0.99]

SPLIT = 0.9         # train/test %

FIGSIZE = (9, 6)


qL1, qL2 = 0.01, 0.10        # percentiles of predictions: lower bounds
qU1, qU2 = 1-qL1, 1-qL2,     # upper bounds derived from lower bounds
label_q1 = f'{int(qU1 * 100)} / {int(qL1 * 100)} percentile band'
label_q2 = f'{int(qU2 * 100)} / {int(qL2 * 100)} percentile band'

mpath = os.path.abspath(os.getcwd()) + SAVE     # path and file name to save the model