import typing as t

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder
from utils import *
from sklearn.linear_model import LinearRegression, LogisticRegression

date_features = ['STARTDATE']
network_features = ['NETWORK']
timelist_features = ['TRANS_LIST']
list_features = ['MONETARY_LIST', 'SESSION_LIST']
numeric_features = [
    'BOD_ACCOUNT_OPEN_USER',
    'BOD_DIRECT_DEPOSIT_USER',
    'HAS_VALID_CREDENTIALS',
    'MOST_RECENT_REQUEST_DECLINE',
    'LAST_MAX_APPROVED_AMOUNT',
    'ADVANCE_TAKEN_AMOUNT',
    'APPROVED_BANK_COUNT',
    'FREQUENCY',
    'T',
    'RECENCY',
    'MONETARY',
    'SESSIONTOTAL'
]
categorical_features = [
    'PLATFORM',
    'ATTRIBUTION',
    'BANK_CATEGORY'
]

feature_list_pipeline = Pipeline(
    steps = [
        ('list_feature_generator', FeatureListGenerator()),
        ('list_feature_standard', StandardScaler())

    ]
)

timefeature_list_pipeline = Pipeline(
    steps = [
        ('time_feature_generator', TimeFeatureListGenerator()),
        ('time_feature_standard', StandardScaler())

    ]
)

date_pipeline = Pipeline(
    steps = [
        ('date_feature_generator', TimeRelatedColumn()),
        ('time_feature_standard', StandardScaler())

    ]
)

numeric_pipeline = Pipeline(
    steps = [
        ('numeric_feature_generator', SimpleImputer(strategy='mean')),
        ('numerica_standard', StandardScaler())

    ]
)

network_pipeline = Pipeline(
    steps = [
        ('remove_none', SimpleImputer(strategy='constant', fill_value='None')),
        ('network_generator', ColumnNameUpdate()),
        ('network_encoder', OneHotEncoder(sparse=False, categories="auto"))

    ]
)

categorical_pipeline = Pipeline(
    steps = [
        ('categorical_generator', SimpleImputer(strategy='constant', fill_value='None')),
        ('categorical_encoder', OneHotEncoder(sparse=False, categories="auto"))
    ]
)



def get_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ('feature_1', feature_list_pipeline, list_features),
            ('feature_2', timefeature_list_pipeline, timelist_features),
            ('feature_3', date_pipeline, date_features),
            ('feature_4', numeric_pipeline, numeric_features),
            ('feature_5', network_pipeline, network_features),
            ('feature_6', categorical_pipeline, categorical_features)
        ]
    )

    return Pipeline(
        steps=[
            ('preprocessor', preprocessor),
            ('linearregression', LinearRegression())
        ]
    )

