import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import linregress
from statsmodels.tsa.stattools import coint

from constants import MAX_HALF_LIFE, WINDOW


class SmartError(Exception):
    pass


def half_life_mean_reversion(series):
    if len(series) <= 1:
        raise SmartError("Series length must be greater than 1.")
    difference = np.diff(series)
    lagged_series = series[:-1]
    slope, _, _, _, _ = linregress(lagged_series, difference)
    if np.abs(slope) < np.finfo(np.float64).eps:
        raise SmartError("Cannot calculate half life. Slope value is too close to zero.")
    half_life = -np.log(2) / slope
    return half_life


# Calculate ZScore
def calculate_zscore(spread):
    spread_series = pd.Series(spread)
    rolling = spread_series.rolling(window=WINDOW)
    rolling_mean = rolling.mean()
    rolling_std = rolling.std()
    zscore = (spread_series - rolling_mean) / rolling_std
    return zscore


# Calculate Cointegration
def calculate_cointegration(series_1, series_2):
    series_1 = np.array(series_1).astype(np.float64)
    series_2 = np.array(series_2).astype(np.float64)
    cointegration_t, p_value, critical_value = coint(series_1, series_2)

    # Better way to fit data vs older version
    series_2_with_constant = sm.add_constant(series_2)
    model = sm.OLS(series_1, series_2_with_constant).fit()
    hedge_ratio = model.params[1]
    intercept = model.params[0]

    spread = series_1 - (series_2 * hedge_ratio) - intercept
    half_life = half_life_mean_reversion(spread)
    t_check = cointegration_t < critical_value[1]
    cointegration_flag = 1 if p_value < 0.05 and t_check else 0
    return cointegration_flag, hedge_ratio, half_life


# Store Cointegration Results
def find_cointegrated_markets_from_all_markets(df_market_prices):
    # Initialize
    markets = df_market_prices.columns.to_list()
    criteria_met_pairs = []

    for index, first_market in enumerate(markets[:-1]):
        series_1 = df_market_prices[first_market].values.astype(np.float64).tolist()
        for second_market in markets[index + 1:]:
            series_2 = df_market_prices[second_market].values.astype(np.float64).tolist()
            cointegration_flag, hedge_ratio, half_life = calculate_cointegration(series_1, series_2)
            if cointegration_flag == 1 and MAX_HALF_LIFE >= half_life > 0:
                criteria_met_pairs.append({
                    "first_market": first_market,
                    "second_market": second_market,
                    "hedge_ratio": hedge_ratio,
                    "half_life": half_life,
                })
    df_criteria_met = pd.DataFrame(criteria_met_pairs)
    return df_criteria_met
