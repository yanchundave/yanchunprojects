import numpy as np
import pandas as pd
from udf import ltv_statistical_model
import missingno as msno
import random
from global_variable import *

# Feature engineering to obtain the needed RFM features
def data_clean():
    df = pd.read_csv(datafile + "advance_user.csv", header=0)
    actual_df = pd.read_csv(datafile + "testdata.csv")
    all_user = pd.read_csv(datafile + "users_property.csv")
    return df, actual_df, all_user

def generate_predict_result(predict_df, actual_df, all_user):
    df_user = all_user.loc[:, ['userid', 'startdate', 'platform', 'attribution', 'network']]
    predict_update = predict_df.loc[:, ['userid', 'first_trans','pred_num', 't_value', 'frequency', 'prob_alive', 'predict_clv']]
    actual_update = actual_df.loc[:, ['userid', 'trans_num', 'real_revenue']]

    combine_1 = pd.merge(predict_update, df_user, on=['userid'], how='left')
    combine_2 = pd.merge(combine_1, actual_update, on=['userid'], how='left')
    combine_2 = combine_2.fillna(0)
    combine_2['start_date'] = pd.to_datetime(combine_2['startdate'])
    combine_2['start_month'] = combine_2['start_date'].apply(lambda x: x.strftime('%Y-%m-01'))
    combine_2['predict_label'] = combine_2['first_trans'].apply(lambda x: 1 if x !=0 else 0)
    combine_2['actual_label'] = combine_2['trans_num'].apply(lambda x: 1 if x != 0 else 0)
    combine_2['tmp_label'] = combine_2['predict_label'] + combine_2['actual_label']
    combine_2['active_label'] = combine_2['tmp_label'].apply(lambda x: 1 if x !=0 else 0)
    combine_2 = combine_2.drop(['tmp_label', 'startdate'], axis=1)

    df = combine_2
    df['churn_predict'] = df['pred_num'].apply(lambda x: 1 if x < 2 else 0)
    df['churn'] = df['real_revenue'].apply(lambda x: 1 if x <0.01 else 0)
    return np.mean(df['t_value']), np.mean(df['churn_predict']), np.mean(df['real_revenue']), np.mean(df['churn']), np.sum(df['real_revenue'])

def main():
    df, actual_df, all_user = data_clean()
    df.columns = ['index_name','userid', 'first_trans', 'frequency', 'T', 'recency', 'monetary']
    arpus = []
    churns = []
    dftotal = ltv_statistical_model(df)

    arpu_predict, churn_predict, arpu, churn, totalrevenue = generate_predict_result(dftotal, actual_df, all_user)
    print("arpu is " + str(arpu_predict))
    print("churn is " + str(churn_predict))
    print("arpu is " + str(arpu))
    print("churn is " + str(churn))
    print("totalrevue is " + str(totalrevenue))
    print("total users is " + str(len(dftotal)))
    # For this part, there is a sql query in sql_script to obtain these metrics
    for i in range(0, 100):
        df_pos = df[(df['T'] > 200)]
        print(i)
        df_ng = df[(df['T'] <= 200)].sample(frac=random.uniform(0, 1), random_state=i)
        dfupdate = pd.concat([df_pos, df_ng], axis=0)
        dftotal = ltv_statistical_model(dfupdate)
        if dftotal is not None:
            tmparpu_predict, tmpchurn_predict, tmparpu, tmpchurn,tmptotal = generate_predict_result(dftotal, actual_df, all_user)
            arpus.append(tmparpu_predict)
            churns.append(tmpchurn_predict)
            print("ltv of this iteration is " + str(tmparpu_predict / tmpchurn_predict ))
    revenue_predict = [arpus[i] / churns[i] + totalrevenue / len(dftotal) for i in range(0, len(churns))]
    result = pd.DataFrame.from_dict({"arpu": arpus, "churn": churns, "predict_ltv":revenue_predict})
    print("arpu is " + str(np.mean(arpus)))
    print("churn is " + str(np.mean(churns)))
    print("arpu is " + str(np.std(arpus)))
    print("churn is " + str(np.std(churns)))
    result.to_csv(datafile + "ltv_test_result.csv")

if __name__ == '__main__':
    main()