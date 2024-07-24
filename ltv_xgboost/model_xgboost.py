import xgboost as xgb
import numpy as np
import pandas as pd
import math
import optuna
from optuna import Trial
from optuna.samplers import TPESampler, QMCSampler
from sklearn.metrics import root_mean_squared_error
from sklearn.metrics import log_loss, roc_auc_score

PATH = "~/Documents/training/learning_datasets/"
np.random.seed(42)

df_raw = pd.read_csv(PATH + 'ltv_training_1.csv')
prediction_xgb = pd.read_csv(PATH + 'ltv_predict_1.csv')

def split_train_test(length, r1, r2):
    shuffle_index = np.random.permutation(length)
    test_size = int(length * r1)
    valid_size = int(length * r2)
    print(test_size)
    print(valid_size)
    test_index = shuffle_index[:test_size]
    valid_index = shuffle_index[test_size: valid_size]
    train_index = shuffle_index[valid_size:]
    return test_index, valid_index, train_index

def log_loss_metric(y_true, y_pred):
    return log_loss(y_true, y_pred)

def train_model(dtrain, dvalid, num_boost_round, params):
    return xgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,
        evals=[(dvalid, "valid")],
        verbose_eval=False,
        early_stopping_rounds=100,
    )

def objective_model(trial: Trial, dtrain, dvalid, objective=None, eval_metric=None):
    params = {
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 1.0, log=True),
        "subsample": trial.suggest_float("subsample", 0.8, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
    }
    if objective:
        params["objective"] = objective
        params["eval_metric"] = eval_metric

    num_boost_round = trial.suggest_int("num_boost_round", 100, 1000)
    model = train_model(dtrain, dvalid, num_boost_round, params)
    preds = model.predict(dvalid)
    score = root_mean_squared_error(dvalid.get_label(), preds)
    return score

def model_input():

    df_xg = df_raw.copy()

    types = {item[0]: item[1] for item in zip(df_raw.dtypes.index, df_raw.dtypes)}
    columns_select = df_raw.columns[3:-7]

    cat_columns = [item for item in columns_select if types[item] == 'object']
    num_columns = [item for item in columns_select if types[item] != 'object']

    df_xg[cat_columns] = df_xg[cat_columns].astype('category')

    test_index, valid_index, train_index = split_train_test(df_xg.shape[0], 0.15, 0.3)
    train = df_xg.iloc[train_index]
    val = df_xg.iloc[valid_index]
    test = df_xg.iloc[test_index]

    return train, val, test, num_columns, cat_columns

def get_best_params(dtrain, dvalid, objective=None, eval_metric=None):
    sampler = QMCSampler(seed=42)
    study = optuna.create_study(
        direction="minimize", sampler=sampler, storage="sqlite:///optuna.db"
    )
    study.optimize(lambda trial: objective_model(trial, dtrain, dvalid, objective, eval_metric), n_trials=60)
    best_params = study.best_params
    return best_params, study.best_trial.params["num_boost_round"]

def get_model(dtrain, dvalid,  y_col):

    if y_col in ['LABEL_REVENUE_6M', 'LABEL_PLEDGE_REVENUE_6M']:
        best_params, num_round = get_best_params(dtrain, dvalid)
    elif y_col == 'LABEL_RETENTION_6M':
        best_params, num_round = get_best_params(dtrain, dvalid, objective="binary:logistic", eval_metric="logloss")
    else:
        print("Not col")

    model = train_model(dtrain, dvalid, num_round, best_params)

    return model

def get_training(train, val, test, x_col, y_col):
    dtrain = xgb.DMatrix(train[x_col], label=train[y_col], enable_categorical=True)
    dvalid = xgb.DMatrix(val[x_col], label=val[y_col], enable_categorical=True)
    dtest = xgb.DMatrix(test[x_col], label=test[y_col], enable_categorical=True)

    model = get_model(dtrain, dvalid, y_col)

    return model, dtest

def get_prediction():

    train, val, test, num_columns, cat_columns = model_input()
    predict_data = prediction_xgb.copy()
    x_col = num_columns + cat_columns

    predict_data[cat_columns] = predict_data[cat_columns].astype('category')
    predict_data[num_columns] = predict_data[num_columns].astype('float')
    pred = xgb.DMatrix(predict_data[x_col], enable_categorical=True)

    # get arpu (net revenue) model
    y_col = 'LABEL_REVENUE_6M'
    model_arpu, dtest = get_training(train, val, test, x_col, y_col)

    predicted_value_arpu = model_arpu.predict(pred)
    predict_data['arpu_predict'] = predicted_value_arpu

    test_value_arpu = model_arpu.predict(dtest)
    test['arpu_predict'] = test_value_arpu

    # get churn model
    y_col_churn = 'LABEL_RETENTION_6M'
    model_churn, dtest_churn = get_training(train, val, test, x_col, y_col_churn)

    predicted_value_churn = model_churn.predict(pred)
    predict_data['churn_predict'] = predicted_value_churn

    test_value_churn = model_churn.predict(dtest_churn)
    test['churn_predict'] = test_value_churn

    # get pledge model
    y_col_pledge = 'LABEL_PLEDGE_REVENUE_6M'
    model_pledge, dtest_pledge = get_training(train, val, test, x_col, y_col_pledge)

    predicted_value_pledge = model_pledge.predict(pred)
    predict_data['pledge_predict'] = predicted_value_pledge

    test_value_pledge = model_churn.predict(dtest_pledge)
    test['churn_predict'] = test_value_pledge
    return predict_data, test

def main():
    predict_data, test = get_prediction()

    print(predict_data['arpu_predict'].mean())
    print(predict_data['churn_predict'].mean())
    print(predict_data['pledge_predict'].mean())


if __name__ == '__main__':
    main()






