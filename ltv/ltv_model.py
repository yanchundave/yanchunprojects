import numpy as np
import pandas as pd
import lifetimes
import missingno as msno
import random
from global_variable import *

# Feature engineering to obtain the needed RFM features
def data_clean():
    df = pd.read_csv(datafile + "advance_user.csv", header=0)
    return df


def ltv_statistical_model(df):
    bgf = lifetimes.BetaGeoFitter(penalizer_coef=0.0)
    bgf.fit(df['frequency'], df['recency'], df['T'])
    df['pred_num'] = bgf.conditional_expected_number_of_purchases_up_to_time(
        predict_t, df['frequency'], df['recency'], df['T'])
    df['monetary_update'] = df['monetary'].apply(lambda x: 0.01 if x==0 else x)
    df['prob_alive'] = bgf.conditional_probability_alive(df['frequency'], df['recency'], df['T'])

    dfupdate = df.loc[df['frequency'] > 0, :]
    #dfupdate = df
    print("frequency and monetary correlation:")
    print(dfupdate[['frequency', 'monetary']].corr())
    bgfupdate = lifetimes.BetaGeoFitter(penalizer_coef=0.05)
    bgfupdate.fit(dfupdate['frequency'], dfupdate['recency'], dfupdate['T'])
    dfupdate['pred_num'] = bgfupdate.conditional_expected_number_of_purchases_up_to_time(
        predict_t, dfupdate['frequency'], dfupdate['recency'], dfupdate['T'])

    ggf = lifetimes.GammaGammaFitter(penalizer_coef=0.0)
    ggf.fit(dfupdate['frequency'], dfupdate['monetary_update'])
    dfupdate['expected_monetary'] = ggf.conditional_expected_average_profit(dfupdate['frequency'], dfupdate['monetary_update'])
    """
    dfupdate['predict_clv'] = ggf.customer_lifetime_value(
        bgf,
        dfupdate['frequency'],
        dfupdate['recency'],
        dfupdate['T'],
        dfupdate['monetary_update'],
        time= 6, # month
        freq = 'D',
        discount_rate=0.01
    )
    """
    dfupdate['t_value'] = dfupdate['pred_num'] * dfupdate['expected_monetary']
    print(sum(dfupdate['t_value']))
    #dfupdate.to_csv(datafile + "dfupdate.csv")

    dfone = df.loc[df['frequency'] == 0, :]
    dfone['expected_monetary'] = dfone['monetary_update']
    dfone['t_value'] = dfone['pred_num'] * dfone['monetary']
    print(sum(dfone['t_value']))

    dftotal = pd.concat([dfupdate, dfone], axis=0)
    print(dftotal.shape)
    dftotal.to_csv(datafile + "dftotal.csv")

def main():
    df = pd.read_csv(datafile + "advance_user.csv", header=0)
    ltv_statistical_model(df)

if __name__ == '__main__':
    main()