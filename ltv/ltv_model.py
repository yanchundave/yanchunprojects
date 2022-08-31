import numpy as np
import pandas as pd
from udf import ltv_statistical_model
import missingno as msno
import random
from global_variable import *

# Feature engineering to obtain the needed RFM features
def data_clean():
    df = pd.read_csv(datafile + "advance_user.csv", header=0)
    return df


def main():
    df = pd.read_csv(datafile + "advance_user.csv", header=0)
    df.columns = ['index_name','userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary']
    dftotal = ltv_statistical_model(df)
    dftotal.to_csv(datafile + "dftotal.csv")


if __name__ == '__main__':
    main()