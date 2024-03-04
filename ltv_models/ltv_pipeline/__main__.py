import numpy as np
import pandas as pd

from pipeline import *
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

FEATURES = ['STARTDATE', 'PLATFORM', 'ATTRIBUTION', 'NETWORK',
       'BOD_ACCOUNT_OPEN_USER', 'BOD_DIRECT_DEPOSIT_USER',
       'BANK_CATEGORY', 'HAS_VALID_CREDENTIALS', 'MOST_RECENT_REQUEST_DECLINE',
       'LAST_MAX_APPROVED_AMOUNT', 'ADVANCE_TAKEN_AMOUNT',
       'APPROVED_BANK_COUNT',  'FREQUENCY', 'T', 'RECENCY',
       'MONETARY', 'TRANS_LIST', 'MONETARY_LIST', 'SESSIONTOTAL',
       'SESSION_LIST', 'REALREVENUE']


def main():
    df = pd.read_csv("pipeline_test.csv", header=0)
    dfupdate = df.loc[:, FEATURES]
    df_train, df_test = split_train_test(dfupdate, 0.1)
    TARGET = "REALREVENUE"

    df_forecast = pd.read_csv("forecast.csv", header=0)
    x_predict = df_forecast.loc[:, FEATURES]


    y_train = df_train["REALREVENUE"]
    x_train = df_train.drop(columns=[TARGET])

    y_test = df_test["REALREVENUE"]
    x_test = df_test.drop(columns=[TARGET])

    rows_with_nan = np.any(np.isnan(y_train))
    print(rows_with_nan)

    pipeline = get_pipeline()
    model = pipeline.fit(x_train, np.nan_to_num(y_train))

    y_pred = model.predict(x_test)
    y_pred[y_pred<0] = 0

    print("arpu is ")
    print(np.mean(y_pred))

    mae = mean_absolute_error(np.nan_to_num(y_test), y_pred)
    mse = mean_squared_error(np.nan_to_num(y_test), y_pred)
    rmse = np.sqrt(mse)  # or you can use mean_squared_error(y_test, y_pred, squared=False)
    r2 = r2_score(np.nan_to_num(y_test), y_pred)

    # Print the metrics
    print(f'Mean Absolute Error (MAE): {mae:.2f}')
    print(f'Mean Squared Error (MSE): {mse:.2f}')
    print(f'Root Mean Squared Error (RMSE): {rmse:.2f}')
    print(f'R-squared (R²): {r2:.2f}')

    y_1 = model.predict(x_predict)
    y_1[y_1<0] = 0
    print("the final average arpu is ")
    print(np.mean(y_1))


if __name__ == '__main__':
    main()