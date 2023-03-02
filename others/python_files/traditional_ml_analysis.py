import numpy as np 
import pandas as pd 
from global_variable import * 


def get_bucket_value():
    value_dic = {}
    value_dic[0] = 0
    for i in range(1, len(values_bucket)):
        value_dic[i] = (values_bucket[i] + values_bucket[i-1]) / 2

    value_dic[len(values_bucket)] = values_bucket[-1] + 100
    return value_dic

def analyze_result(df, df_origin, df_x_y, value_dic):
    dfupdate = pd.merge(df, df_origin.loc[:, ['userid']], on=['userid'])
    df_test = pd.merge(dfupdate, df_x_y, on=['userid'])
    df_test['predict_value'] = df_test['predicted_class'].apply(lambda x: value_dic[int(x)])

    prediction_sum = np.sum(df_test['predict_value'])
    actual_sum = np.sum(df_test['revenue'])

    predicted_arpu = prediction_sum / len(df_test)
    actual_arpu = actual_sum/ len(df_test)

    rsquare_coefficient = np.square(np.corrcoef(df_test['predict_value'], df_test['revenue']))

    print("The test volume is " + str(len(df_test)))
    print("The predicted_arpu is " + str(predicted_arpu))
    print("The actual arpu is " + str(actual_arpu))
    print("The rsqare is " + str(rsquare_coefficient))

    return df_test
   
def compare_with_lifetime(df_test):
    df_ltv = df = pd.read_csv("/Users/yanchunyang/Documents/datafiles/ltv/" + "dftotal.csv", header=0)
    df_selected = pd.merge(df_ltv, df_test.loc[:, ['userid', 'revenue', 'predict_value']])
    print(df_selected.shape)

    rsquare_coefficient = np.square(np.corrcoef(df_selected['t_value'], df_selected['revenue']))
    print("The rsquare is " + str(rsquare_coefficient))

    rsquare_coefficient_ml = np.square(np.corrcoef(df_selected['predict_value'], df_selected['revenue']))
    print("The rsquare is " + str(rsquare_coefficient_ml))


def main():
    df = pd.read_csv(datafile_path + "test_set.csv", header=0)
    df_origin = pd.read_csv(datafile_path + "df_origin.csv", header=0)
    df_x_y = pd.read_csv(datafile_path + "df_x_y.csv", header=0)
    value_dic = get_bucket_value()

    df_test = analyze_result(df, df_origin, df_x_y, value_dic)
    #compare_with_lifetime(df_test)

if __name__ == '__main__':
    main()
    