import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import pickle

def main():
    df = pd.read_csv("/Users/yanchunyang/Documents/training/datasets/housing.csv")
    df.columns = ['income', 'age', 'rooms', 'bedrooms', 'population', 'price', 'address']
    dfupdate = df.loc[:, ['income', 'age', 'rooms', 'bedrooms', 'population', 'price']]
    lin_reg = LinearRegression()
    lin_reg.fit(dfupdate.loc[:, ['income', 'age', 'rooms', 'bedrooms', 'population']], dfupdate['price'])
    pickle.dump( lin_reg, open( "model.p", "wb" ) )
    sample = dfupdate.iloc[:5]
    sample_example = sample.loc[:,['income', 'age', 'rooms', 'bedrooms', 'population'] ]
    sample_example.to_csv("sample.txt")
    print("training done")

if __name__ == '__main__':
    main()
