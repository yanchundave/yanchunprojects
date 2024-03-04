import typing as t

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin

import numpy as np
import math

class ColumnNameUpdate(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.channels = [
            "Adwords",
            "Apple Search Ads",
            "Facebook",
            "Organic",
            "Referral",
            "Snapchat",
            "bytedanceglobal_int",
        ]

    def fit(self, X, Y=None):
        return self

    def transform(self, X):
        update_col = np.where(np.isin(X[:, 0], self.channels), X[:, 0] + '_NETWORK', 'NETWORK_OTHERS' )
        return np.c_[update_col]

class TimeRelatedColumn(BaseEstimator, TransformerMixin):

    def fit(self, X, Y=None):
        return self

    def transform(self, X):
        month_col = np.array([float(x[5:7]) for x in X.iloc[:, 0]])
        sine_month = np.sin(2 * math.pi * month_col / 12)
        cos_month = np.cos(2 * math.pi * month_col / 12)
        return np.c_[month_col, sine_month, cos_month]


class FeatureListGenerator(BaseEstimator, TransformerMixin):
    def fit(self, X, Y=None):
        return self

    def _calculate_std(self, item):
        try:
            if item is None or np.isnan(item) or (not isinstance(item, str) and  math.isnan(item)):
                #print(item)
                return float(-1)
            splits = str(item).strip().split(",")
            if len(splits) >= 2:
                values = [float(x) for x in splits]
                if np.mean(values) == 0:
                    std = float(0)
                else:
                    std = np.std(values) / np.mean(values)
            else:
                std = float(0)
            return std
        except:
            return float(-1)


    def _calculate_listlength(self, item):
        if item is None or (isinstance(item, float) and  math.isnan(item)):
            return_len = 0
        else:
            splits = item.strip().split(",")
            return_len = float(len(splits))
        return return_len

    def transform(self, X):
        print("feature list is " + str(type(X)))
        vectorized_std = np.vectorize(self._calculate_std)
        vectorized_length = np.vectorize(self._calculate_listlength)
        monetary_std = vectorized_std(X)
        activemonth = vectorized_length(X)
        return np.c_[monetary_std, activemonth]

class TimeFeatureListGenerator(BaseEstimator, TransformerMixin):
    def fit(self, X, Y=None):
        return self

    def _calculate_std_derived(self, item):

        if len(item) == 0 or item is None or (not isinstance(item, str) and  math.isnan(item)):
            std = float(-1)
        else:
            splits = item.strip().split(",")
            if len(splits) >= 3:
                values = [float(x) for x in splits]
                values.sort()
                values_update = [y - x for x, y in zip(values, values[1:])]
                if np.mean(values_update) == 0:
                    std = float(0)
                else:
                    std = np.std(values_update) / np.mean(values_update)

            else:
                std = float(0)
        return std

    def transform(self, X):
        print("timelist is " + str(type(X)))
        vectorized_std_derived = np.vectorize(self._calculate_std_derived)
        timediff_std = vectorized_std_derived(X)

        return np.c_[timediff_std]


def split_train_test(df, ratio):
    shuffle_index = np.random.permutation(len(df))
    test_size = int(len(df) * ratio)
    test_index = shuffle_index[:test_size]
    train_index = shuffle_index[test_size:]
    return df.iloc[train_index], df.iloc[test_index]