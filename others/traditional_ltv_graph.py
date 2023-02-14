import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt 
from global_variable import *

"""
This script is specially for presentation. 
On July 29, we have a project progress with Leader team so have to prepare some graphs for presentation.
The script is to generate some graphs which help leader team to understand.
"""

def draw_graph(df, bucketname):
    item_list = list(set(df[bucketname]))
    print(item_list)
    color_list = ['aqua', 'blue', 'blueviolet', 'darkolivegreen', 'gold', 'darkgoldenrod', 'chocolate']
    plt.subplot(1, 1, 1)
    for index, subitem in enumerate(item_list):
        df_sub = df.loc[df[bucketname] == subitem, :].sort_values(by=['month'])
        plt.plot(df_sub['month'], df_sub['avg_revenue'], color_list[index], label=subitem)
    plt.xticks(rotation=75)
    plt.legend()
    plt.title(bucketname + " and average revenue")
    plt.xlabel("user age")
    plt.ylabel("avverage revenue")
    plt.savefig(datafile_path + bucketname + ".png")
    plt.close()

def get_bucket(x, buckets):
    for i in range(1, len(buckets)):
        if x < buckets[i]:
            return str(buckets[i-1]) + "-" + str(buckets[i])
    return "over " + str(buckets[-1])

def data_clean(df):
    frequency_bucket = [0, 5, 10, 20]
    monetary_bucket = [0, 5, 10, 20]
    recency_bucket = [0, 50, 100, 150, 200, 350]
    df['frequency_bucket'] = df['frequency'].apply(lambda x: get_bucket(x, frequency_bucket))
    df['monetary_bucket'] = df['monetary'].apply(lambda x: get_bucket(x, monetary_bucket))
    df['recency_bucket'] = df['recency'].apply(lambda x: get_bucket(x, recency_bucket))
    df['month'] = df['startdate'].apply(lambda x: str(x)[0:7])
    print(df.head(4))

    for item in ['frequency_bucket', 'monetary_bucket', 'recency_bucket']:
        df_selected = df.loc[:, ['userid', 'month', item, 'revenue']]
        df_groupby = df_selected.groupby(['month', item]).agg({'revenue':'sum', 'userid':'count'}).reset_index()
        df_groupby['avg_revenue'] = df_groupby['revenue'] / df_groupby['userid']
        draw_graph(df_groupby, item)
       

def main():
    df = pd.read_csv(datafile_path + "df_x_y.csv", header=0)
    print(df.columns)
    data_clean(df)
    
if __name__ == '__main__':
    main()