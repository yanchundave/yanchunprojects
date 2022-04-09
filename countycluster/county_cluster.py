import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

datafile = "/Users/yanchunyang/Documents/datafiles/"

def draw_elbow(df_pre):
    result = []
    for k in range(2, 10):
        kmeans = KMeans(n_clusters=k)
        kmeans.fit(df_pre)
        result.append([k, kmeans.inertia_])
    x = [x[0] for x in result]
    y = [x[1] for y in result]
    plt.plot(x, y)
    plt.show()

def cluster_analysis(census_file):
    df = pd.read_csv(datafile + census_file)
    df = df.fillna(1)
    df['bratio'] = df['black_population'].astype(float) / df['population'].astype(float)
    df['property_ratio'] = df['income_below_poverty'].astype(float) / df['population'].astype(float)
    df_1 = df.loc[:, ['population', 'median_income']].values
    df_2 = df.loc[:, ['bratio', 'property_ratio']].values
    scaler = MinMaxScaler()
    df_1_scaler = scaler.fit_transform(df_1)
    data_pre = np.concatenate([df_2, df_1_scaler], axis=1)
    n = 5  # after analyzing the elbow graph to define 5 
    kmeans = KMeans(n_clusters=n)
    y_pred = kmeans.fit_predict(data_pre)
    df['cluster'] = y_pred
    df.to_csv(datafile + "county_cluster.csv")

def main():
    census_file = "census_data.csv"
    cluster_analysis(census_file)

if __name__ == '__main__':
    main()