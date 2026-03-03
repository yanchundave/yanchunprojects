#!/usr/bin/env python3
"""PFA-367: Manual settlement rate by past-due bucket over time."""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

buckets = pd.read_csv("past_due_buckets.csv")
buckets["DISB_MONTH"] = pd.to_datetime(buckets["DISB_MONTH"])
buckets = buckets[buckets["DISB_MONTH"] < "2026-02-01"]

# Line chart: manual % by bucket over time
fig, ax = plt.subplots(figsize=(14, 7))

bucket_colors = {
    "01: 0-30 days": "#2563eb",
    "02: 30-60 days": "#16a34a",
    "03: 60-180 days": "#f59e0b",
    "04: 180-360 days": "#f97316",
    "05: 360+ days": "#dc2626",
}

for bucket, color in bucket_colors.items():
    subset = buckets[buckets["PAST_DUE_BUCKET"] == bucket].sort_values("DISB_MONTH")
    if len(subset) > 0:
        label = bucket.split(": ")[1]
        ax.plot(subset["DISB_MONTH"], subset["PCT_MANUAL_USERS"],
                marker="o", color=color, linewidth=2, markersize=4, label=label)

ax.set_title("Manual Settlement Rate by Past Due Bucket (Monthly Trend)", fontsize=14, fontweight="bold")
ax.set_xlabel("Disbursement Month")
ax.set_ylabel("% Manual Settlement Among Past Due Users")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.legend(title="Days Past Due", fontsize=10, title_fontsize=11)
ax.grid(True, alpha=0.3)
ax.tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("past_due_bucket_trend.png", dpi=150)
print("  -> past_due_bucket_trend.png")
