import numpy as np
import pandas as pd
from darts.models import RegressionModel
from darts import TimeSeries, concatenate
from darts.dataprocessing.transformers import Scaler
from global_var import *
from darts.utils.timeseries_generation import datetime_attribute_timeseries

df = pd.read_csv("rp_ts.csv", header=0)
spending = pd.read_csv("rp_spending.csv", header=0)
spending.loc[spending['DATE']=='2021-03-31','SPEND'] = 95000
spending.columns = ['startdate', 'spending']
df.columns = ['startdate', 'usertotal', 'revenue', 'sessiontotal', 'banktotal']
df['arpu'] = df['revenue'] / df['usertotal']
df = df.iloc[0:-20]
df_spend = pd.merge(df, spending, on=['startdate'], how='left')
dfupdate = df_spend.loc[:, ['startdate', 'sessiontotal', 'banktotal', 'spending', 'arpu']]
dfupdate = dfupdate.fillna(0)
dfupdate['start_date'] = pd.to_datetime(dfupdate['startdate'])
ts_p = TimeSeries.from_dataframe(dfupdate, time_col='start_date', value_cols='arpu')
ts_cov = TimeSeries.from_dataframe(dfupdate, time_col='start_date', value_cols=['sessiontotal', 'banktotal'])
ts_future_cov = TimeSeries.from_dataframe(spending, time_col='startdate', value_cols=['spending'])

ts_train, ts_test = ts_p.split_after(SPLIT)
scalerP = Scaler()
scalerP.fit_transform(ts_train)
ts_ttrain = scalerP.transform(ts_train)
ts_ttest = scalerP.transform(ts_test)
ts_t = scalerP.transform(ts_p)
ts_t = ts_t.astype(np.float32)
ts_ttrain = ts_ttrain.astype(np.float32)
ts_ttest = ts_ttest.astype(np.float32)

covF_train, covF_test = ts_cov.split_after(SPLIT)

scalerF = Scaler()
scalerF.fit_transform(covF_train)
covF_ttrain = scalerF.transform(covF_train)
covF_ttest = scalerF.transform(covF_test)
covF_t = scalerF.transform(ts_cov)

# make sure data are of type float
covF_ttrain = covF_ttrain.astype(np.float32)
covF_ttest = covF_ttest.astype(np.float32)

covT = datetime_attribute_timeseries( ts_p.time_index, attribute="day", add_length=23 )   # 48 hours beyond end of test set to prepare for out-of-sample forecasting
covT = covT.stack(  datetime_attribute_timeseries(covT.time_index, attribute="day_of_week")  )
covT = covT.stack(  datetime_attribute_timeseries(covT.time_index, attribute="month")  )

covT = covT.add_holidays(country_code="US")
covT = covT.astype(np.float32)
future_cov = covT.stack(ts_future_cov)
covF_future_train, covF_future_test =future_cov.split_after(SPLIT)

scalerF_future = Scaler()
scalerF_future.fit_transform(covF_future_train)
covF_future_ttrain = scalerF_future.transform(covF_future_train)
covF_future_ttest = scalerF_future.transform(covF_future_test)
covF_future_t = scalerF_future.transform(future_cov)

# make sure data are of type float
covF_future_ttrain = covF_future_ttrain.astype(np.float32)
covF_future_ttest = covF_future_ttest.astype(np.float32)

pd.options.display.float_format = '{:.2f}'.format
print("first and last row of scaled feature covariates:")
covF_future_t.pd_dataframe().iloc[[0,-1]]

model = RegressionModel(lags=[-7, -6, -5, -4, -3, -2, -1],
                        lags_past_covariates=[-7, -6, -5, -4, -3, -2, -1],
                        lags_future_covariates=[-7, -6, -5, -4, -3, -2, -1, 0],
                        output_chunk_length=7)

model.fit(ts_ttrain,
          past_covariates=covF_ttrain,
          future_covariates=covF_future_ttrain)

pred = model.predict(n=60,
                     past_covariates=covF_t,
                     future_covariates=covF_future_t)

pred = scalerP.inverse_transform(pred)

t = ts_p.plot()
p = pred.plot()

print("Done")