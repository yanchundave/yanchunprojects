#!/usr/bin/env python3
"""PFA-367 v2: Revised visualizations per requirements."""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

monthly = pd.read_csv("past_due_monthly.csv")
monthly["DISB_MONTH"] = pd.to_datetime(monthly["DISB_MONTH"])
monthly = monthly[monthly["DISB_MONTH"] < "2026-02-01"]

buckets = pd.read_csv("past_due_buckets.csv")
buckets["DISB_MONTH"] = pd.to_datetime(buckets["DISB_MONTH"])
buckets = buckets[buckets["DISB_MONTH"] < "2026-02-01"]

x = np.arange(len(monthly))
month_labels = monthly["DISB_MONTH"].dt.strftime("%Y-%m").values
bar_width = 0.35

# ============================================================
# Chart 1: Total Users vs Past Due Users + Rate line
# ============================================================
fig, ax1 = plt.subplots(figsize=(16, 7))

b1 = ax1.bar(x - bar_width/2, monthly["TOTAL_USERS"], bar_width,
             label="Total Overdraft Users", color="#93c5fd", edgecolor="white")
b2 = ax1.bar(x + bar_width/2, monthly["PAST_DUE_USERS"], bar_width,
             label="Past Due Users", color="#2563eb", edgecolor="white")

ax1.set_xlabel("Disbursement Month", fontsize=11)
ax1.set_ylabel("Users", fontsize=11)
ax1.set_xticks(x)
ax1.set_xticklabels(month_labels, rotation=45, ha="right", fontsize=9)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e6:.1f}M"))
ax1.legend(loc="upper left", fontsize=10)
ax1.grid(True, alpha=0.2, axis="y")

ax2 = ax1.twinx()
line = ax2.plot(x, monthly["PCT_PAST_DUE_USERS"], color="#dc2626", marker="o",
                linewidth=2.5, markersize=5, label="Past Due Rate (%)", zorder=5)
for i, val in enumerate(monthly["PCT_PAST_DUE_USERS"]):
    ax2.annotate(f"{val:.1f}%", (x[i], val), textcoords="offset points",
                 xytext=(0, 10), ha="center", fontsize=7.5, color="#dc2626", fontweight="bold")

ax2.set_ylabel("Past Due Rate (%)", fontsize=11, color="#dc2626")
ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax2.tick_params(axis="y", labelcolor="#dc2626")
ax2.set_ylim(0, 60)
ax2.legend(loc="upper right", fontsize=10)

ax1.set_title("Monthly Overdraft Users vs Past Due Users", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("chart1_users_vs_pastdue.png", dpi=150)
print("  -> chart1_users_vs_pastdue.png")
plt.close()

# ============================================================
# Chart 2: Past Due Users vs Manual Settlement Users + Rate line
# ============================================================
fig, ax1 = plt.subplots(figsize=(16, 7))

b1 = ax1.bar(x - bar_width/2, monthly["PAST_DUE_USERS"], bar_width,
             label="Past Due Users", color="#fdba74", edgecolor="white")
b2 = ax1.bar(x + bar_width/2, monthly["PAST_DUE_MANUAL_USERS"], bar_width,
             label="Manual Settlement Users", color="#f97316", edgecolor="white")

ax1.set_xlabel("Disbursement Month", fontsize=11)
ax1.set_ylabel("Users", fontsize=11)
ax1.set_xticks(x)
ax1.set_xticklabels(month_labels, rotation=45, ha="right", fontsize=9)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e6:.1f}M" if v >= 1e6 else f"{v/1e3:.0f}K"))
ax1.legend(loc="upper left", fontsize=10)
ax1.grid(True, alpha=0.2, axis="y")

ax2 = ax1.twinx()
line = ax2.plot(x, monthly["PCT_MANUAL_AMONG_PAST_DUE_USERS"], color="#dc2626", marker="o",
                linewidth=2.5, markersize=5, label="Manual Settlement Rate (%)", zorder=5)
for i, val in enumerate(monthly["PCT_MANUAL_AMONG_PAST_DUE_USERS"]):
    ax2.annotate(f"{val:.1f}%", (x[i], val), textcoords="offset points",
                 xytext=(0, 10), ha="center", fontsize=7.5, color="#dc2626", fontweight="bold")

ax2.set_ylabel("Manual Settlement Rate (%)", fontsize=11, color="#dc2626")
ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax2.tick_params(axis="y", labelcolor="#dc2626")
ax2.set_ylim(0, 30)
ax2.legend(loc="upper right", fontsize=10)

ax1.set_title("Past Due Users vs Manual Settlement Users", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("chart2_pastdue_vs_manual.png", dpi=150)
print("  -> chart2_pastdue_vs_manual.png")
plt.close()

# ============================================================
# Chart 3: Bucket share of manual settlement users (sums to 100%)
# ============================================================
bucket_order = ["01: 0-30 days", "02: 30-60 days", "03: 60-180 days", "04: 180-360 days", "05: 360+ days"]
bucket_colors = {
    "01: 0-30 days": "#2563eb",
    "02: 30-60 days": "#16a34a",
    "03: 60-180 days": "#f59e0b",
    "04: 180-360 days": "#f97316",
    "05: 360+ days": "#dc2626",
}

# For each month, compute each bucket's share of total manual users
monthly_total_manual = buckets.groupby("DISB_MONTH")["MANUAL_SETTLEMENT_USERS"].sum().reset_index()
monthly_total_manual.columns = ["DISB_MONTH", "TOTAL_MANUAL"]

bucket_share = buckets.merge(monthly_total_manual, on="DISB_MONTH")
bucket_share["SHARE"] = bucket_share["MANUAL_SETTLEMENT_USERS"] / bucket_share["TOTAL_MANUAL"] * 100

fig, ax = plt.subplots(figsize=(16, 7))

for bucket in bucket_order:
    subset = bucket_share[bucket_share["PAST_DUE_BUCKET"] == bucket].sort_values("DISB_MONTH")
    if len(subset) > 0:
        label = bucket.split(": ")[1]
        ax.plot(subset["DISB_MONTH"], subset["SHARE"],
                marker="o", color=bucket_colors[bucket], linewidth=2, markersize=5, label=label)

ax.set_title("Distribution of Manual Settlements Across Past Due Buckets\n(Each month sums to 100%)",
             fontsize=14, fontweight="bold")
ax.set_xlabel("Disbursement Month", fontsize=11)
ax.set_ylabel("Share of Manual Settlement Users (%)", fontsize=11)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.legend(title="Days Past Due", fontsize=10, title_fontsize=11, loc="center right")
ax.grid(True, alpha=0.3)
ax.tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("chart3_bucket_share.png", dpi=150)
print("  -> chart3_bucket_share.png")
plt.close()

# ============================================================
# Chart 4 (bonus): Stacked area — composition of past due users by bucket
# ============================================================
fig, ax = plt.subplots(figsize=(16, 7))

pivot = buckets.pivot_table(index="DISB_MONTH", columns="PAST_DUE_BUCKET",
                            values="PAST_DUE_USERS", fill_value=0)
# Ensure column order
for b in bucket_order:
    if b not in pivot.columns:
        pivot[b] = 0
pivot = pivot[bucket_order]

# Normalize to 100%
pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

colors = [bucket_colors[b] for b in bucket_order]
labels = [b.split(": ")[1] for b in bucket_order]

ax.stackplot(pivot_pct.index, [pivot_pct[b] for b in bucket_order],
             labels=labels, colors=colors, alpha=0.85)
ax.set_title("Composition of Past Due Users by Bucket (% of Total Past Due)",
             fontsize=14, fontweight="bold")
ax.set_xlabel("Disbursement Month", fontsize=11)
ax.set_ylabel("Share (%)", fontsize=11)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.legend(title="Days Past Due", fontsize=10, title_fontsize=11, loc="center right")
ax.grid(True, alpha=0.3, axis="y")
ax.tick_params(axis="x", rotation=45)
ax.set_ylim(0, 100)

plt.tight_layout()
plt.savefig("chart4_pastdue_composition.png", dpi=150)
print("  -> chart4_pastdue_composition.png")
plt.close()

# ============================================================
# Chart 5 (bonus): Manual settlement rate by bucket (bar chart, latest month)
# ============================================================
latest_month = buckets["DISB_MONTH"].max()
# Use a month that has all 5 buckets
full_month = buckets[buckets["DISB_MONTH"] == "2025-07-01"]
if len(full_month) < 5:
    full_month = buckets[buckets["DISB_MONTH"] == "2024-12-01"]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Left: manual rate by bucket for a representative month
bucket_labels = [b.split(": ")[1] for b in bucket_order]
rates = []
volumes = []
for b in bucket_order:
    row = full_month[full_month["PAST_DUE_BUCKET"] == b]
    if len(row) > 0:
        rates.append(row.iloc[0]["PCT_MANUAL_USERS"])
        volumes.append(row.iloc[0]["PAST_DUE_USERS"])
    else:
        rates.append(0)
        volumes.append(0)

colors_bar = [bucket_colors[b] for b in bucket_order]
bars = ax1.bar(bucket_labels, rates, color=colors_bar, edgecolor="white", linewidth=0.5)
for bar, val in zip(bars, rates):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
             f"{val:.1f}%", ha="center", fontsize=10, fontweight="bold")
ax1.set_title(f"Manual Settlement Rate by Bucket\n({full_month.iloc[0]['DISB_MONTH'].strftime('%b %Y')} Cohort)",
              fontsize=13, fontweight="bold")
ax1.set_ylabel("% Manual Settlement")
ax1.set_xlabel("Days Past Due")
ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax1.grid(True, alpha=0.3, axis="y")

# Right: avg days to manual settlement (approximation using bucket midpoints)
# Show volume distribution as horizontal bar
total_v = sum(volumes)
pcts = [v/total_v*100 for v in volumes]
bars2 = ax2.barh(bucket_labels[::-1], [p for p in pcts[::-1]],
                 color=[c for c in colors_bar[::-1]], edgecolor="white")
for bar, val in zip(bars2, pcts[::-1]):
    ax2.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
             f"{val:.1f}%", va="center", fontsize=10, fontweight="bold")
ax2.set_title(f"Past Due Volume Distribution by Bucket\n({full_month.iloc[0]['DISB_MONTH'].strftime('%b %Y')} Cohort)",
              fontsize=13, fontweight="bold")
ax2.set_xlabel("% of Past Due Users")
ax2.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax2.grid(True, alpha=0.3, axis="x")

plt.tight_layout()
plt.savefig("chart5_bucket_detail.png", dpi=150)
print("  -> chart5_bucket_detail.png")
plt.close()

# ============================================================
# Print insights
# ============================================================
print("\n" + "="*60)
print("INSIGHTS")
print("="*60)

# Trend analysis
first_half = monthly[monthly["DISB_MONTH"] < "2025-01-01"]["PCT_PAST_DUE_USERS"].mean()
second_half = monthly[monthly["DISB_MONTH"] >= "2025-01-01"]["PCT_PAST_DUE_USERS"].mean()
print(f"\n1. Past due rate trend:")
print(f"   2024 avg: {first_half:.1f}%  |  2025+ avg: {second_half:.1f}%  |  Change: +{second_half-first_half:.1f}pp")

# Seasonality
dec_months = monthly[monthly["DISB_MONTH"].dt.month == 12]["PCT_PAST_DUE_USERS"].mean()
non_dec = monthly[monthly["DISB_MONTH"].dt.month != 12]["PCT_PAST_DUE_USERS"].mean()
print(f"\n2. December seasonality:")
print(f"   Dec avg: {dec_months:.1f}%  |  Non-Dec avg: {non_dec:.1f}%  |  Dec premium: +{dec_months-non_dec:.1f}pp")

# Manual rate stability
manual_std = monthly["PCT_MANUAL_AMONG_PAST_DUE_USERS"].std()
manual_mean = monthly["PCT_MANUAL_AMONG_PAST_DUE_USERS"].mean()
print(f"\n3. Manual settlement rate stability:")
print(f"   Mean: {manual_mean:.1f}%  |  Std: {manual_std:.2f}pp  |  CV: {manual_std/manual_mean*100:.1f}%")

# Bucket concentration
bucket_agg = buckets.groupby("PAST_DUE_BUCKET").agg(
    total_manual=("MANUAL_SETTLEMENT_USERS", "sum"),
    total_pd=("PAST_DUE_USERS", "sum")
).reset_index()
total_manual_all = bucket_agg["total_manual"].sum()
b030 = bucket_agg[bucket_agg["PAST_DUE_BUCKET"] == "01: 0-30 days"]["total_manual"].values[0]
print(f"\n4. Manual settlement concentration:")
print(f"   0-30 day bucket: {b030/total_manual_all*100:.1f}% of all manual settlements")
print(f"   Total manual settlement users (all months): {total_manual_all:,.0f}")

# Growth in past due
first_users = monthly.iloc[0]["PAST_DUE_USERS"]
last_users = monthly.iloc[-1]["PAST_DUE_USERS"]
print(f"\n5. Past due user growth:")
print(f"   Jan 2024: {first_users:,.0f}  |  Jan 2026: {last_users:,.0f}  |  Growth: {(last_users/first_users-1)*100:.0f}%")
