import numpy as np 
import pandas as pd 
import pickle
from udf import read_from_database
from global_variable import *
from sklearn.cluster import KMeans
from collections import Counter

with open(datafile_path + "vocab.pk", 'rb') as f:
        vocab = pickle.load(f)

feature_array = pd.read_csv(datafile_path + "weights_ide.csv", header=None).values

def extract_data():

    with open(datafile_path + "vocab.pk", 'rb') as f:
        vocab = pickle.load(f)

    sql_str = """
    select * from DBT.DEV_YANCHUN_PUBLIC.USER_EVENT_STR;
    """
    result = read_from_database(sql_str)
    df = pd.DataFrame(result, columns=['USER_ID', 'STARTTIME', 'EVENTSTRING'])
    df.to_csv(datafile_path + "user_event_raw.csv", sep='|')

    return df, vocab 

def calculate_features(eventstr):
    splits = eventstr.strip().split(",")
    features_index = [vocab[x] if x in vocab else 0 for x in splits ]
    features = [feature_array[i,:] for i in features_index]
    features_array = np.array(features).mean(axis=0).astype(str)
    if len(features_array) != 24:
        print(eventstr)
    return ",".join(features_array)

def generate_feature():
    df = pd.read_csv(datafile_path + "user_event_raw.csv", header=0, sep='|')
    df['features'] = df['EVENTSTRING'].apply(lambda x: calculate_features(x))
    dfupdate = df.loc[:, ['USER_ID', 'STARTTIME', 'features']]
    dfupdate.to_csv(datafile_path + "user_event_features.csv")
    print(dfupdate.head(10))

def get_kmeans():
    k = 10
    kmeans = KMeans(n_clusters=k)
    kmeans.fit(feature_array)
    return kmeans

def get_ratio(model, eventstr):
    splits = eventstr.strip().split(",")
    features_index = [vocab[x] if x in vocab else 0 for x in splits ]
    features = [feature_array[i,:] for i in features_index]
    y_pred = model.predict(features)
    count_dic = Counter(y_pred)
    result = [count_dic[i] for i in range(0, 10)]
    result.append(len(y_pred))
    return result


def get_center_feature(kmeans_model):
    results = []
    i = 0
    with open(datafile_path +  "user_event_raw.csv", 'r') as f:
        for line in f.readlines():
            if i == 0:
                i += 1
                continue
            tmp = []
            splits = line.strip().split("|")
            tmp.append(splits[1])
            tmp.append(splits[2])
            tmp += get_ratio(kmeans_model, splits[3])
            results.append(tmp)
    cols = ["c" + str(i) for i in range(1, 12)]
    total_cols = ['userid', 'starttime'] + cols
    df = pd.DataFrame(results, columns=total_cols)
    df.to_csv(datafile_path + "user_feature_update.csv")


def main():
    #extract_data()
    """
    kmean_model = get_kmeans()
    with open(datafile_path + "kmeancenter.pk", 'wb') as f:
        pickle.dump(kmean_model.cluster_centers_, f)
    """
    #get_center_feature(kmean_model)
    generate_feature()
    print("user event feature is ready")

if __name__ == '__main__':
    main()