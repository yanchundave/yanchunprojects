import lifetimes
import pandas as pd

PRED_T = 180
def ltv_stats_model(df):
    try:
        bgf = lifetimes.BetaGeoFitter(penalizer_coef=0.0)
        bgf.fit(df['FREQUENCY'], df['RECENCY'], df['T'])
        df['PRED_NUM'] = bgf.conditional_expected_number_of_purchases_up_to_time(
            PRED_T, df['FREQUENCY'], df['RECENCY'], df['T'])
        df['MONETARY_UPDATE'] = df['MONETARY'].apply(lambda x: 0.01 if x==0 else x)
        df['PROB_ALIVE'] = bgf.conditional_probability_alive(df['FREQUENCY'], df['RECENCY'], df['T'])

        dfupdate = df.loc[df['FREQUENCY'] > 0, :]

        bgfupdate = lifetimes.BetaGeoFitter(penalizer_coef=0.05)
        bgfupdate.fit(dfupdate['FREQUENCY'], dfupdate['RECENCY'], dfupdate['T'])
        dfupdate['PRED_NUM'] = bgfupdate.conditional_expected_number_of_purchases_up_to_time(
            PRED_T, dfupdate['FREQUENCY'], dfupdate['RECENCY'], dfupdate['T'])

        ggf = lifetimes.GammaGammaFitter(penalizer_coef=0.0)
        ggf.fit(dfupdate['FREQUENCY'], dfupdate['MONETARY_UPDATE'])
        dfupdate['EXPECTED_MONETARY'] = ggf.conditional_expected_average_profit(dfupdate['FREQUENCY'], dfupdate['MONETARY_UPDATE'])
        bgf.fit(dfupdate['FREQUENCY'], dfupdate['RECENCY'], dfupdate['T'])
        dfupdate['PREDICT_CLV'] = ggf.customer_lifetime_value(
            bgf,
            dfupdate['FREQUENCY'],
            dfupdate['RECENCY'],
            dfupdate['T'],
            dfupdate['MONETARY_UPDATE'],
            time= 6, # month
            freq = 'D',
            discount_rate=0.01
        )

        dfupdate['T_VALUE'] = dfupdate['PRED_NUM'] * dfupdate['EXPECTED_MONETARY']

        dfone = df.loc[df['FREQUENCY'] == 0, :]
        dfone['EXPECTED_MONETARY'] = dfone['MONETARY_UPDATE']
        dfone['T_VALUE'] = dfone['PRED_NUM'] * dfone['MONETARY']

        dftotal = pd.concat([dfupdate, dfone], axis=0)
        return dftotal.loc[:, ['USER_ID', 'T_VALUE', 'PRED_NUM', 'EXPECTED_MONETARY', 'PREDICT_CLV']]
    except:
        return None