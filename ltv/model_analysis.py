import numpy as np
import pandas as pd
import missingno as msno
import random
from global_variable import *
import matplotlib.pyplot as plt
from scipy.stats import norm

def draw_norm( mean, sd, xmin, xmax, ymin, ymax, actual=None):
    x = np.arange(xmin, xmax, 0.1)
    plt.plot(x, norm.pdf(x, mean, sd), color='green')
    if actual:
        plt.vlines(x=actual, ymin=ymin, ymax=ymax, color= 'blue', label="Forecast LTV")
        plt.vlines(x=mean, ymin=ymin, ymax=ymax, color= 'green', linestyles='dashdot', label="Expected LTV")
        plt.title("LTV forecast on test dataset")
    else:
        x_percentile = mean - 1.645 * sd
        plt.vlines(x=mean, ymin=ymin, ymax=ymax, color= 'blue')
        plt.vlines(x = mean-1.645 * sd, ymin=ymin, ymax=ymax, color='blue', linestyles='dashdot', label='5% percentile')
        plt.vlines(x = mean+1.645 * sd, ymin=ymin, ymax=ymax, color='blue', linestyles='dashdot', label='95% percentile')
        plt.title("LTV forecast currently")
    plt.legend()
    if actual:
        plt.savefig(datafile + "forecast_test.png")
    else:
        plt.savefig(datafile + "forcast.png")
    plt.show()

def get_mean_std(df, df_acquired):
    arpu = list(df['arpu'])
    churn = list(df['churn'])
    result = []
    """
    for i in range(0, len(arpu)):
        for j in range(0, len(churn)):
            result.append(arpu[i] / churn[j] + df_acquired)
    """
    result = [arpu[i] / churn[i] + df_acquired for i in range(0, len(arpu))]
    result.append(np.min(df['arpu'])/np.max(df['churn']) + df_acquired)
    result.append(np.max(df['arpu'])/np.min(df['churn']) + df_acquired)

    mean = np.mean(result)
    std = np.std(result )
    return mean, std


def main():
    dftest = pd.read_csv(datafile +  "ltv_test_result.csv")
    dfforecast = pd.read_csv(datafile + "forecast_sample.csv")
    #from sql of sql_script
    dftest_acquired = 19.88
    dfforecast_acquired = 52.77

    test_mean, test_std = get_mean_std(dftest, dftest_acquired)
    actual = 57.25
    print("test_mean is " + str(test_mean))
    print("test_std is " + str(test_std))
    z_score = (actual - test_mean) / test_std
    p_value = norm.sf(z_score) * 2
    print("p value is " + str(p_value))
    draw_norm(test_mean, test_std, test_mean - 3*test_std, test_mean + 3*test_std, -0.01, 0.15, actual)

    forecast_mean, forecast_std = get_mean_std(dfforecast, dfforecast_acquired)
    forecast = 85.81
    draw_norm(forecast, forecast_std, forecast - 2 * forecast_std, forecast + 2 * forecast_std, -0.01, 0.2)


if __name__ == '__main__':
    main()

