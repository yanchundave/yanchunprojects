import pandas as pd

def pivot_table(df, network):
    dfupdate = pd.pivot_table(df, columns=['platform'], index=['date'],
        values=['spending'], aggfunc='sum', fill_value=0)
    dfupdate.columns = [network + '_' + x[0] + '_' + x[1] for x in dfupdate.columns]
    return dfupdate