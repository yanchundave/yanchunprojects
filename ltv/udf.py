import jaydebeapi as jay
import lifetimes
import pandas as pd


predict_t = 180
def read_from_snowflake(sql_str):
    with open('/Users/yanchunyang/pwd/snowflake.passphrase', 'r') as f:
        passphrase = f.read().strip()
    username = "yanchun.yang@dave.com"
    password = "abc"
    jdbcpath = "/Users/yanchunyang/lib/jdbc/snowflake-jdbc-3.13.8.jar"
    jdbc_driver_name = "net.snowflake.client.jdbc.SnowflakeDriver"
    hostname= "qc63563.snowflakecomputing.com"
    role = "FUNC_ACCOUNTING_USER"
    warehouse = "ACCOUNTING_WH"
    keyfile = "/Users/yanchunyang/.ssh/snowflake.p8"

    conn_string = f'jdbc:snowflake://qc63563.snowflakecomputing.com?role={role}&warehouse={warehouse}&private_key_file={keyfile}&private_key_file_pwd={passphrase}'

    conn = jay.connect(jdbc_driver_name, conn_string, {'user': username , 'password': password }, jars=jdbcpath)

#  Currently python can't interpret correctly the result returned from JDBC to connect Snowflake so we have to switch back to JSON rather than ARROW format
# It can be done at session level
    session_set = "ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'"
    curs = conn.cursor()
    curs.execute(session_set)

    curs.execute(sql_str)
    result = curs.fetchall()
    return result

def ltv_statistical_model(df):
    try:
        bgf = lifetimes.BetaGeoFitter(penalizer_coef=0.0)
        bgf.fit(df['frequency'], df['recency'], df['T'])
        df['pred_num'] = bgf.conditional_expected_number_of_purchases_up_to_time(
            predict_t, df['frequency'], df['recency'], df['T'])
        df['monetary_update'] = df['monetary'].apply(lambda x: 0.01 if x==0 else x)
        df['prob_alive'] = bgf.conditional_probability_alive(df['frequency'], df['recency'], df['T'])

        dfupdate = df.loc[df['frequency'] > 0, :]

        #dfupdate = df
        #print("frequency and monetary correlation:")
        #print(dfupdate[['frequency', 'monetary']].corr())
        bgfupdate = lifetimes.BetaGeoFitter(penalizer_coef=0.05)
        bgfupdate.fit(dfupdate['frequency'], dfupdate['recency'], dfupdate['T'])
        dfupdate['pred_num'] = bgfupdate.conditional_expected_number_of_purchases_up_to_time(
            predict_t, dfupdate['frequency'], dfupdate['recency'], dfupdate['T'])

        ggf = lifetimes.GammaGammaFitter(penalizer_coef=0.0)
        ggf.fit(dfupdate['frequency'], dfupdate['monetary_update'])
        dfupdate['expected_monetary'] = ggf.conditional_expected_average_profit(dfupdate['frequency'], dfupdate['monetary_update'])
        bgf.fit(dfupdate['frequency'], dfupdate['recency'], dfupdate['T'])
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

        dfupdate['t_value'] = dfupdate['pred_num'] * dfupdate['expected_monetary']
        #print(sum(dfupdate['t_value']))
        #dfupdate.to_csv(datafile + "dfupdate.csv")

        dfone = df.loc[df['frequency'] == 0, :]
        dfone['expected_monetary'] = dfone['monetary_update']
        dfone['t_value'] = dfone['pred_num'] * dfone['monetary']
        #print(sum(dfone['t_value']))

        dftotal = pd.concat([dfupdate, dfone], axis=0)
        #print(dftotal.shape)
        return dftotal
    except:
        return None