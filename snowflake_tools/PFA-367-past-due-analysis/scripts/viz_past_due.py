#!/usr/bin/env python3
"""PFA-367: Visualize past due rates and manual settlement rates."""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# Load data
monthly = pd.read_csv("past_due_monthly.csv")
monthly["DISB_MONTH"] = pd.to_datetime(monthly["DISB_MONTH"])

# Exclude Feb 2026 (incomplete month)
monthly = monthly[monthly["DISB_MONTH"] < "2026-02-01"]

buckets = pd.read_csv("past_due_buckets.csv")
buckets["DISB_MONTH"] = pd.to_datetime(buckets["DISB_MONTH"])
buckets = buckets[buckets["DISB_MONTH"] < "2026-02-01"]

# ============================================================
# Chart 1: Past Due Users / Total Users (monthly)
# ============================================================
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

ax1.plot(monthly["DISB_MONTH"], monthly["PCT_PAST_DUE_USERS"],
         marker="o", color="#2563eb", linewidth=2, markersize=5, label="% Past Due Users")
ax1.fill_between(monthly["DISB_MONTH"], monthly["PCT_PAST_DUE_USERS"],
                 alpha=0.15, color="#2563eb")

ax1.set_title("Past Due Users as % of Total Overdraft Users", fontsize=14, fontweight="bold")
ax1.set_ylabel("% Past Due")
ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax1.grid(True, alpha=0.3)
ax1.legend(loc="upper left", fontsize=10)

# Add value labels on key points
for i in [0, len(monthly)//2, len(monthly)-1]:
    row = monthly.iloc[i]
    ax1.annotate(f"{row['PCT_PAST_DUE_USERS']:.1f}%",
                 (row["DISB_MONTH"], row["PCT_PAST_DUE_USERS"]),
                 textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8, color="#2563eb")

# ============================================================
# Chart 2: Manual Settlement % Among Past Due Users (monthly)
# ============================================================
ax2.plot(monthly["DISB_MONTH"], monthly["PCT_MANUAL_AMONG_PAST_DUE_USERS"],
         marker="s", color="#dc2626", linewidth=2, markersize=5, label="% Manual Settlement Among Past Due")
ax2.fill_between(monthly["DISB_MONTH"], monthly["PCT_MANUAL_AMONG_PAST_DUE_USERS"],
                 alpha=0.15, color="#dc2626")

ax2.set_title("Manual Settlement % Among Past Due Users", fontsize=14, fontweight="bold")
ax2.set_ylabel("% Manual Settlement")
ax2.set_xlabel("Disbursement Month")
ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax2.grid(True, alpha=0.3)
ax2.legend(loc="upper left", fontsize=10)
ax2.tick_params(axis="x", rotation=45)

for i in [0, len(monthly)//2, len(monthly)-1]:
    row = monthly.iloc[i]
    ax2.annotate(f"{row['PCT_MANUAL_AMONG_PAST_DUE_USERS']:.1f}%",
                 (row["DISB_MONTH"], row["PCT_MANUAL_AMONG_PAST_DUE_USERS"]),
                 textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8, color="#dc2626")

plt.tight_layout()
plt.savefig("past_due_rates.png", dpi=150)
print("  -> past_due_rates.png")

# ============================================================
# Chart 3: Manual Settlement % by Past Due Bucket (stacked/grouped)
# ============================================================
# Aggregate across all months for overall bucket view
bucket_agg = buckets.groupby("PAST_DUE_BUCKET").agg(
    total_past_due=("PAST_DUE_USERS", "sum"),
    total_manual=("MANUAL_SETTLEMENT_USERS", "sum")
).reset_index()
bucket_agg["pct_manual"] = bucket_agg["total_manual"] / bucket_agg["total_past_due"] * 100

fig2, (ax3, ax4) = plt.subplots(1, 2, figsize=(16, 6))

# Left: Bar chart of manual % by bucket
bucket_labels = [b.split(": ")[1] for b in bucket_agg["PAST_DUE_BUCKET"]]
colors = ["#60a5fa", "#3b82f6", "#2563eb", "#1d4ed8", "#1e3a8a"]
bars = ax3.bar(bucket_labels, bucket_agg["pct_manual"], color=colors, edgecolor="white", linewidth=0.5)

for bar, val in zip(bars, bucket_agg["pct_manual"]):
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
             f"{val:.1f}%", ha="center", fontsize=10, fontweight="bold")

ax3.set_title("Manual Settlement % by Past Due Bucket\n(All Months Combined)", fontsize=13, fontweight="bold")
ax3.set_ylabel("% Manual Settlement")
ax3.set_xlabel("Days Past Due")
ax3.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax3.grid(True, alpha=0.3, axis="y")

# Right: Stacked bar showing volume by bucket
ax4.bar(bucket_labels, bucket_agg["total_manual"], color="#dc2626", label="Manual Settlement", edgecolor="white")
ax4.bar(bucket_labels, bucket_agg["total_past_due"] - bucket_agg["total_manual"],
        bottom=bucket_agg["total_manual"], color="#94a3b8", label="Auto/Other Settlement", edgecolor="white")

for i, (label, total) in enumerate(zip(bucket_labels, bucket_agg["total_past_due"])):
    ax4.text(i, total + total*0.01, f"{total/1e6:.1f}M", ha="center", fontsize=9, fontweight="bold")

ax4.set_title("Past Due Volume by Bucket\n(All Months Combined)", fontsize=13, fontweight="bold")
ax4.set_ylabel("Users")
ax4.set_xlabel("Days Past Due")
ax4.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
ax4.legend(fontsize=10)
ax4.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
plt.savefig("past_due_buckets.png", dpi=150)
print("  -> past_due_buckets.png")

# Print summary
print("\nMonthly Summary (latest complete month - Jan 2026):")
latest = monthly[monthly["DISB_MONTH"] == "2026-01-01"].iloc[0]
print(f"  Total overdraft users:    {latest['TOTAL_USERS']:>12,.0f}")
print(f"  Past due users:           {latest['PAST_DUE_USERS']:>12,.0f} ({latest['PCT_PAST_DUE_USERS']:.1f}%)")
print(f"  Manual among past due:    {latest['PAST_DUE_MANUAL_USERS']:>12,.0f} ({latest['PCT_MANUAL_AMONG_PAST_DUE_USERS']:.1f}%)")

print("\nBucket Summary (all months combined):")
for _, row in bucket_agg.iterrows():
    print(f"  {row['PAST_DUE_BUCKET']:20s}  {row['total_past_due']:>10,.0f} past due  {row['pct_manual']:>6.1f}% manual")
