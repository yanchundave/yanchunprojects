import utils
import pandas as pd
from data_clean import *
import preprocessing
import lightweight_mmm, plot, optimize_media, media_transforms
import jax.numpy as jnp
from sklearn.metrics import mean_absolute_percentage_error
import matplotlib.pyplot as plt
from optimize_media import find_optimal_budgets

FLAG = 1
common_path = "/Users/yanchunyang/Documents/datafiles/pystan/"
datafile_path = common_path + "user/" if FLAG==1 else common_path + "revenue/"

def read_data():
    common_path = "/Users/yanchunyang/Documents/datafiles/pystan/"
    datafile_path = common_path + "user/" if FLAG==1 else common_path + "revenue/"
    pv_daily = "platform_user_advance.csv" if FLAG == 1 else "total_revenue.csv"
    spending_daily = "channel_spending_raw.csv"
    df_pv = pd.read_csv(datafile_path + pv_daily)
    spending_pv = pd.read_csv(datafile_path + spending_daily)
    print("input done")
    return df_pv, spending_pv


def train_test():
    response, spending = read_data()
    spending_update, spending_channels = channel_combine(spending)

    cost = spending_update.sum(axis=0)
    response = response.fillna(0)
    spending_update = spending_update.fillna(0)


    split_time = pd.Timestamp("2022-12-15")
    spending_update['date_update'] = pd.to_datetime(spending_update['datenumber'], format='%Y-%m-%d')
    response['date_update'] = pd.to_datetime(response['date'], format='%Y-%m-%d')
    spending_train = spending_update.loc[spending_update['date_update'] < split_time - pd.Timedelta(1, 'D'), spending_channels]
    spending_test = spending_update.loc[spending_update['date_update'] >= split_time, spending_channels]

    response_train = response.loc[response['date_update'] < split_time - pd.Timedelta(1, 'D'), ['PV']]
    response_test = response.loc[response['date_update'] >= split_time, ['PV']]

    cost_train = spending_train.loc[:, spending_channels].sum(axis=0)
    cost_test = spending_test.loc[:, spending_channels].sum(axis=0)

    spending_scaler = preprocessing.CustomScaler(divide_operation=jnp.mean)
    response_scaler = preprocessing.CustomScaler(divide_operation=jnp.mean)
    cost_scaler = preprocessing.CustomScaler(divide_operation=jnp.mean)

    spending_train_scaled = spending_scaler.fit_transform(spending_train.values)
    response_train_scaled = response_scaler.fit_transform(response_train.values.squeeze())
    cost_train_scaled = cost_scaler.fit_transform(cost_train.values)

    spending_test_scaled = spending_scaler.transform(spending_test.values)

    adstock_model = ['hill_adstock']
    degree_season = [1]



    mmm = lightweight_mmm.LightweightMMM(model_name='hill_adstock')
    mmm.fit(
            media=spending_train_scaled,
            media_prior=cost_train_scaled,
            target=response_train_scaled,
            number_warmup=1000,
            number_samples=1000,
            number_chains=1,
            degrees_seasonality=1,
            weekday_seasonality=True,
            seasonality_frequency=365,
            seed=1
        )

    prediction = mmm.predict(
        media=spending_test_scaled,
        target_scaler=response_scaler
    )
    p = prediction.mean(axis=0)

    mape = mean_absolute_percentage_error(response_test.values, p)
    print(f"model_name='hill_adstock' degrees=1 MAPE={mape} samples={p[:3]}")

    channel_contribution, roi_hat = mmm.get_posterior_metrics()

    channel_graph = plot.plot_bars_media_metrics(metric=roi_hat, channel_names=spending_channels)
    mediachannel = plot.plot_media_channel_posteriors(media_mix_model=mmm, channel_names=spending_channels)

    print(spending_train_scaled.shape)
    print(response_train_scaled.shape)
    print(cost_train_scaled.shape)
    print("run here")


def main():
    response, spending = read_data()
    spending_update, spending_channels = channel_combine(spending)

    cost = spending_update.sum(axis=0)
    response = response.fillna(0)
    spending_update = spending_update.fillna(0)

    spending_update['date_update'] = pd.to_datetime(spending_update['datenumber'], format='%Y-%m-%d')
    response['date_update'] = pd.to_datetime(response['date'], format='%Y-%m-%d')
    spending_data = spending_update.loc[:, spending_channels]
    response_data = response.loc[:, ['PV']]
    cost_data = spending_data.loc[:, spending_channels].sum(axis=0)

    spending_scaler = preprocessing.CustomScaler(divide_operation=jnp.mean)
    response_scaler = preprocessing.CustomScaler(divide_operation=jnp.mean)
    cost_scaler = preprocessing.CustomScaler(divide_operation=jnp.mean)

    spending_data_scaled = spending_scaler.fit_transform(spending_data.values)
    response_data_scaled = response_scaler.fit_transform(response_data.values.squeeze())
    cost_data_scaled = cost_scaler.fit_transform(cost_data.values)

    adstock_model = ['hill_adstock']
    degree_season = [1]

    mmm = lightweight_mmm.LightweightMMM(model_name='hill_adstock')
    mmm.fit(
            media=spending_data_scaled,
            media_prior=cost_data_scaled,
            target=response_data_scaled,
            number_warmup=1000,
            number_samples=1000,
            number_chains=1,
            degrees_seasonality=1,
            weekday_seasonality=True,
            seasonality_frequency=365,
            seed=1
        )

    channel_contribution, roi_hat = mmm.get_posterior_metrics(cost_scaler=cost_scaler, target_scaler=response_scaler)

    prices = jnp.repeat(1,len(spending_channels))

    solution, kpi_without_optim, starting_values = find_optimal_budgets(
    n_time_periods=100,
    media_mix_model=mmm,
    budget=19000000,
    prices=prices,
    target_scaler=response_scaler,
    media_scaler=spending_scaler,
    bounds_lower_pct=0.8,
    bounds_upper_pct=1.2,
    max_iterations=200,
    solver_func_tolerance=1e-06,
    solver_step_size=1.4901161193847656e-08,
    seed=10)

    channel_graph = plot.plot_bars_media_metrics(metric=roi_hat, channel_names=spending_channels)
    mediachannel_graph = plot.plot_media_channel_posteriors(media_mix_model=mmm, channel_names=spending_channels)
    channel_graph.savefig(datafile_path + "lm_media.png")
    mediachannel_graph.savefig(datafile_path + "lm_channel.png")

    print("run here")

if __name__ == '__main__':
    main()