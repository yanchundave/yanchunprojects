#!/usr/bin/env python3
"""Time series forecast: predict next month user counts for rent payers vs non-rent payers."""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from statsmodels.tsa.holtwinters import ExponentialSmoothing

print("Loading data...")
df = pd.read_csv("mtm_rent_extracash_results.csv", usecols=["USER_ID", "CURRENT_MONTH", "IF_PAID_RENT"])
df["CURRENT_MONTH"] = pd.to_datetime(df["CURRENT_MONTH"])

# Aggregate monthly counts
monthly = df.groupby(["CURRENT_MONTH", "IF_PAID_RENT"]).agg(
    users=("USER_ID", "count")
).reset_index()

# Separate rent payers and non-rent payers
rent = monthly[monthly["IF_PAID_RENT"] == 1].set_index("CURRENT_MONTH")["users"].sort_index()
non_rent = monthly[monthly["IF_PAID_RENT"] == 0].set_index("CURRENT_MONTH")["users"].sort_index()

# Set frequency to month-start
rent.index = pd.DatetimeIndex(rent.index, freq="MS")
non_rent.index = pd.DatetimeIndex(non_rent.index, freq="MS")

print(f"Historical data: {rent.index[0].strftime('%Y-%m')} to {rent.index[-1].strftime('%Y-%m')} ({len(rent)} months)")

# Fit Holt-Winters Exponential Smoothing (additive trend, no seasonality since <2 full years)
# Using additive trend with damping for more conservative forecasts
print("Fitting models...")

model_rent = ExponentialSmoothing(
    rent, trend="add", damped_trend=True, seasonal=None
).fit(optimized=True)

model_non_rent = ExponentialSmoothing(
    non_rent, trend="add", damped_trend=True, seasonal=None
).fit(optimized=True)

# Forecast next 3 months (Jan, Feb, Mar 2026) for context, but highlight Jan 2026
n_forecast = 3
forecast_rent = model_rent.forecast(n_forecast)
forecast_non_rent = model_non_rent.forecast(n_forecast)

print(f"\nForecast (Holt-Winters Exponential Smoothing, additive damped trend):")
print(f"  {'Month':<12} {'Rent Payers':>14} {'Non-Rent Payers':>16} {'Total':>12}")
print(f"  {'-'*56}")
for i in range(n_forecast):
    m = forecast_rent.index[i].strftime("%Y-%m")
    r = forecast_rent.iloc[i]
    nr = forecast_non_rent.iloc[i]
    print(f"  {m:<12} {r:>14,.0f} {nr:>16,.0f} {r+nr:>12,.0f}")

# Also print fitted values vs actuals for model quality check
print(f"\nModel fit (last 3 months):")
for i in range(-3, 0):
    m = rent.index[i].strftime("%Y-%m")
    r_actual = rent.iloc[i]
    r_fitted = model_rent.fittedvalues.iloc[i]
    nr_actual = non_rent.iloc[i]
    nr_fitted = model_non_rent.fittedvalues.iloc[i]
    print(f"  {m}: Rent actual={r_actual:,.0f} fitted={r_fitted:,.0f} ({(r_fitted/r_actual-1)*100:+.1f}%)  |  Non-Rent actual={nr_actual:,.0f} fitted={nr_fitted:,.0f} ({(nr_fitted/nr_actual-1)*100:+.1f}%)")

# ============================================================
# Plot
# ============================================================
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

# Colors
color_rent = "#2563eb"
color_non_rent = "#f97316"
color_forecast = "#dc2626"

# --- Top: Rent Payers ---
ax1.plot(rent.index, rent.values, marker="o", color=color_rent, linewidth=2,
         markersize=6, label="Rent Payers (Historical)", zorder=3)

# Connect last historical point to forecast
bridge_rent = pd.concat([rent.iloc[-1:], forecast_rent])
ax1.plot(bridge_rent.index, bridge_rent.values, marker="D", color=color_forecast,
         linewidth=2, markersize=8, linestyle="--", label="Rent Payers (Forecast)", zorder=4)
# Remove the first bridge point marker (it's the last historical point)
ax1.plot(rent.index[-1], rent.iloc[-1], marker="o", color=color_rent, markersize=6, zorder=5)

# Add value labels on forecast points
for idx, val in forecast_rent.items():
    ax1.annotate(f"{val:,.0f}", (idx, val), textcoords="offset points",
                 xytext=(0, 12), ha="center", fontsize=9, color=color_forecast, fontweight="bold")

ax1.set_title("Rent Payers: Historical + Forecast", fontsize=14, fontweight="bold")
ax1.set_ylabel("Users")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e3:.0f}K"))
ax1.legend(loc="upper left", fontsize=10)
ax1.grid(True, alpha=0.3)

# --- Bottom: Non-Rent Payers ---
ax2.plot(non_rent.index, non_rent.values, marker="o", color=color_non_rent, linewidth=2,
         markersize=6, label="Non-Rent Payers (Historical)", zorder=3)

bridge_non_rent = pd.concat([non_rent.iloc[-1:], forecast_non_rent])
ax2.plot(bridge_non_rent.index, bridge_non_rent.values, marker="D", color=color_forecast,
         linewidth=2, markersize=8, linestyle="--", label="Non-Rent Payers (Forecast)", zorder=4)
ax2.plot(non_rent.index[-1], non_rent.iloc[-1], marker="o", color=color_non_rent, markersize=6, zorder=5)

for idx, val in forecast_non_rent.items():
    ax2.annotate(f"{val/1e6:.2f}M", (idx, val), textcoords="offset points",
                 xytext=(0, 12), ha="center", fontsize=9, color=color_forecast, fontweight="bold")

ax2.set_title("Non-Rent Payers: Historical + Forecast", fontsize=14, fontweight="bold")
ax2.set_ylabel("Users")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
ax2.legend(loc="upper left", fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.tick_params(axis="x", rotation=45)

# Add method note
fig.text(0.5, 0.01,
         "Model: Holt-Winters Exponential Smoothing (additive damped trend) | Red diamonds = forecast",
         ha="center", fontsize=9, color="#64748b")

plt.tight_layout(rect=[0, 0.03, 1, 1])
plt.savefig("forecast_rent_users.png", dpi=150)
print("\n  -> forecast_rent_users.png")
