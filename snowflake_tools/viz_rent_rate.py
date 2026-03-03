#!/usr/bin/env python3
"""Chart: Monthly rate of rent payers among MTM users."""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# Load data
df = pd.read_csv("mtm_rent_extracash_results.csv", usecols=["USER_ID", "CURRENT_MONTH", "IF_PAID_RENT"])

# Aggregate by month
monthly = df.groupby("CURRENT_MONTH").agg(
    total_users=("USER_ID", "count"),
    rent_users=("IF_PAID_RENT", "sum"),
).reset_index()

monthly["rent_rate"] = monthly["rent_users"] / monthly["total_users"]
monthly["non_rent_rate"] = 1 - monthly["rent_rate"]

# Plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(monthly["CURRENT_MONTH"], monthly["rent_rate"], marker="o", label="Paid Rent", linewidth=2)
ax.plot(monthly["CURRENT_MONTH"], monthly["non_rent_rate"], marker="s", label="Did Not Pay Rent", linewidth=2)

ax.set_xlabel("Month")
ax.set_ylabel("Rate (% of MTM Users)")
ax.set_title("Monthly Rate: Rent Payers vs Non-Rent Payers Among MTM Users")
ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=1))
ax.legend()
ax.grid(True, alpha=0.3)
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("rent_rate_monthly.png", dpi=150)
print("Chart saved to rent_rate_monthly.png")
print()
print(monthly[["CURRENT_MONTH", "total_users", "rent_users", "rent_rate"]].to_string(index=False))
