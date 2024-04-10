import os
import pandas as pd
import numpy as np
import math
import logging

log = logging.getLogger(os.path.basename(__file__))
logging.basicConfig(level=logging.INFO)

categorical_features = ["PLATFORM", "ATTRIBUTION"]


def get_trending(row):
    X = np.array([float(x) for x in row["MONTHLIST"].strip().split(",")])
    Y = np.array([float(x) for x in row["REVLIST"].strip().split(",")])
    if len(X) == 0 or len(X) == 1:
        return 0, 0

    trending_ratio = np.sum((X - np.mean(X)) * (Y - np.mean(Y))) / (
        np.sum(np.square(X - np.mean(X))) + 0.001
    )
    intercept = np.mean(Y) - trending_ratio * np.mean(X)
    return trending_ratio, intercept


def get_transformation(df):
    df[["trending", "intercept"]] = df.apply(
        lambda row: get_trending(row), axis=1
    ).apply(pd.Series)
    df["forecastdate"] = pd.to_datetime(df["FORECAST_DATE"])
    df["month"] = df["forecastdate"].dt.month
    df["month_sin"] = np.sin(2 * math.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * math.pi * df["month"] / 12)
    df_data = pd.get_dummies(
        df,
        prefix=["platform", "attribution"],
        prefix_sep="_",
        dummy_na=True,
        columns=categorical_features,
        sparse=False,
        drop_first=False,
        dtype=None,
    )
    df_data.drop(
        columns=["FORECAST_DATE", "BANK_CATEGORY", "FIRST_TRANS", "NETWORK"],
        inplace=True,
    )
    return df_data
