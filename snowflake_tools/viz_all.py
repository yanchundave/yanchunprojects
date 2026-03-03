#!/usr/bin/env python3
"""Generate all analysis charts: competitor breakdown, EC comparison, N+5 retention."""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

print("Loading data...")
df = pd.read_csv("mtm_rent_extracash_results.csv")
df["CURRENT_MONTH"] = pd.to_datetime(df["CURRENT_MONTH"])

# ============================================================
# Chart 1: Competitor breakdown among rent payers
# ============================================================
print("Chart 1: Competitor breakdown...")
rent_df = df[df["IF_PAID_RENT"] == 1].copy()

competitor_monthly = rent_df.groupby(["CURRENT_MONTH", "COMPETITOR"]).agg(
    users=("USER_ID", "count")
).reset_index()

total_rent_monthly = rent_df.groupby("CURRENT_MONTH").agg(
    total_rent_users=("USER_ID", "count")
).reset_index()

competitor_monthly = competitor_monthly.merge(total_rent_monthly, on="CURRENT_MONTH")
competitor_monthly["share"] = competitor_monthly["users"] / competitor_monthly["total_rent_users"]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))

# Left: stacked area of user counts
competitors = ["Other", "Flex Finance", "Bilt", "Rent App", "Livable", "Zillow"]
colors = ["#94a3b8", "#2563eb", "#dc2626", "#16a34a", "#9333ea", "#f59e0b"]

pivot_counts = competitor_monthly.pivot(index="CURRENT_MONTH", columns="COMPETITOR", values="users").fillna(0)
for c in competitors:
    if c not in pivot_counts.columns:
        pivot_counts[c] = 0
pivot_counts = pivot_counts[competitors]

ax1.stackplot(pivot_counts.index, [pivot_counts[c] for c in competitors],
              labels=competitors, colors=colors, alpha=0.8)
ax1.set_title("Rent Payer Count by Competitor")
ax1.set_xlabel("Month")
ax1.set_ylabel("Users")
ax1.legend(loc="upper left", fontsize=9)
ax1.grid(True, alpha=0.3)
ax1.tick_params(axis="x", rotation=45)

# Right: line chart of share %
for comp, color in zip(competitors, colors):
    subset = competitor_monthly[competitor_monthly["COMPETITOR"] == comp]
    if len(subset) > 0:
        ax2.plot(subset["CURRENT_MONTH"], subset["share"], marker="o", label=comp,
                 color=color, linewidth=2, markersize=4)

ax2.set_title("Competitor Share Among Rent Payers")
ax2.set_xlabel("Month")
ax2.set_ylabel("Share of Rent Payers")
ax2.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=1))
ax2.legend(loc="center right", fontsize=9)
ax2.grid(True, alpha=0.3)
ax2.tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("competitor_breakdown.png", dpi=150)
print("  -> competitor_breakdown.png")

# Print competitor summary
print("\n  Competitor share (latest month):")
latest = competitor_monthly[competitor_monthly["CURRENT_MONTH"] == competitor_monthly["CURRENT_MONTH"].max()]
for _, row in latest.sort_values("share", ascending=False).iterrows():
    print(f"    {row['COMPETITOR']:15s} {row['users']:>8,.0f} users  ({row['share']:.1%})")

# ============================================================
# Chart 2: ExtraCash comparison - rent payers vs non-rent payers
# ============================================================
print("\nChart 2: ExtraCash comparison...")

ec_comparison = df.groupby(["CURRENT_MONTH", "IF_PAID_RENT"]).agg(
    total_users=("USER_ID", "count"),
    ec_users=("EXTRACASH_TRANSACTIONS", lambda x: (x > 0).sum()),
    avg_ec_txns=("EXTRACASH_TRANSACTIONS", "mean"),
    avg_ec_disbursement=("EXTRACASH_DISBURSEMENT", "mean"),
    total_ec_disbursement=("EXTRACASH_DISBURSEMENT", "sum"),
).reset_index()

ec_comparison["ec_adoption_rate"] = ec_comparison["ec_users"] / ec_comparison["total_users"]
ec_comparison["label"] = ec_comparison["IF_PAID_RENT"].map({1: "Rent Payers", 0: "Non-Rent Payers"})

fig, axes = plt.subplots(1, 3, figsize=(20, 6))

# 2a: EC adoption rate
for label, color in [("Rent Payers", "#2563eb"), ("Non-Rent Payers", "#f97316")]:
    subset = ec_comparison[ec_comparison["label"] == label]
    axes[0].plot(subset["CURRENT_MONTH"], subset["ec_adoption_rate"],
                 marker="o", label=label, color=color, linewidth=2, markersize=4)
axes[0].set_title("ExtraCash Adoption Rate")
axes[0].set_ylabel("% of Users with EC Transaction")
axes[0].yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))
axes[0].legend()
axes[0].grid(True, alpha=0.3)
axes[0].tick_params(axis="x", rotation=45)

# 2b: Avg EC transactions per user
for label, color in [("Rent Payers", "#2563eb"), ("Non-Rent Payers", "#f97316")]:
    subset = ec_comparison[ec_comparison["label"] == label]
    axes[1].plot(subset["CURRENT_MONTH"], subset["avg_ec_txns"],
                 marker="o", label=label, color=color, linewidth=2, markersize=4)
axes[1].set_title("Avg ExtraCash Transactions per User")
axes[1].set_ylabel("Avg Transactions")
axes[1].legend()
axes[1].grid(True, alpha=0.3)
axes[1].tick_params(axis="x", rotation=45)

# 2c: Avg EC disbursement per user
for label, color in [("Rent Payers", "#2563eb"), ("Non-Rent Payers", "#f97316")]:
    subset = ec_comparison[ec_comparison["label"] == label]
    axes[2].plot(subset["CURRENT_MONTH"], subset["avg_ec_disbursement"],
                 marker="o", label=label, color=color, linewidth=2, markersize=4)
axes[2].set_title("Avg ExtraCash Disbursement per User")
axes[2].set_ylabel("Avg Disbursement ($)")
axes[2].legend()
axes[2].grid(True, alpha=0.3)
axes[2].tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("ec_comparison.png", dpi=150)
print("  -> ec_comparison.png")

# Print EC summary
print("\n  EC comparison (latest month):")
latest_ec = ec_comparison[ec_comparison["CURRENT_MONTH"] == ec_comparison["CURRENT_MONTH"].max()]
for _, row in latest_ec.iterrows():
    print(f"    {row['label']:20s} adoption={row['ec_adoption_rate']:.1%}  avg_txns={row['avg_ec_txns']:.2f}  avg_disb=${row['avg_ec_disbursement']:.2f}")

# ============================================================
# Chart 3: N+5 month ExtraCash retention
# ============================================================
print("\nChart 3: N+5 retention...")

# Only use months where N+5 data is available (up to Jul 2025)
retention_df = df[df["CURRENT_MONTH"] <= "2025-07-01"].copy()

retention = retention_df.groupby(["CURRENT_MONTH", "IF_PAID_RENT"]).agg(
    total_users=("USER_ID", "count"),
    users_with_ec_now=("EXTRACASH_TRANSACTIONS", lambda x: (x > 0).sum()),
    users_with_ec_n5=("AFTER_5_MONTH_EXTRACASH_TRANSACTIONS", lambda x: (x > 0).sum()),
    avg_ec_txns_now=("EXTRACASH_TRANSACTIONS", "mean"),
    avg_ec_txns_n5=("AFTER_5_MONTH_EXTRACASH_TRANSACTIONS", "mean"),
    avg_disb_now=("EXTRACASH_DISBURSEMENT", "mean"),
    avg_disb_n5=("AFTER_5_MONTH_DISBURSEMENT", "mean"),
).reset_index()

retention["ec_rate_now"] = retention["users_with_ec_now"] / retention["total_users"]
retention["ec_rate_n5"] = retention["users_with_ec_n5"] / retention["total_users"]
retention["label"] = retention["IF_PAID_RENT"].map({1: "Rent Payers", 0: "Non-Rent Payers"})

fig, axes = plt.subplots(1, 3, figsize=(20, 6))

# 3a: EC adoption rate now vs N+5
for label, color, style in [("Rent Payers", "#2563eb", "-"), ("Non-Rent Payers", "#f97316", "-")]:
    subset = retention[retention["label"] == label]
    axes[0].plot(subset["CURRENT_MONTH"], subset["ec_rate_now"],
                 marker="o", label=f"{label} (Month N)", color=color, linewidth=2, markersize=4)
    axes[0].plot(subset["CURRENT_MONTH"], subset["ec_rate_n5"],
                 marker="s", label=f"{label} (Month N+5)", color=color, linewidth=2,
                 markersize=4, linestyle="--", alpha=0.7)
axes[0].set_title("EC Adoption: Month N vs N+5")
axes[0].set_ylabel("% of Users with EC")
axes[0].yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))
axes[0].legend(fontsize=8)
axes[0].grid(True, alpha=0.3)
axes[0].tick_params(axis="x", rotation=45)

# 3b: Avg EC transactions now vs N+5
for label, color in [("Rent Payers", "#2563eb"), ("Non-Rent Payers", "#f97316")]:
    subset = retention[retention["label"] == label]
    axes[1].plot(subset["CURRENT_MONTH"], subset["avg_ec_txns_now"],
                 marker="o", label=f"{label} (Month N)", color=color, linewidth=2, markersize=4)
    axes[1].plot(subset["CURRENT_MONTH"], subset["avg_ec_txns_n5"],
                 marker="s", label=f"{label} (Month N+5)", color=color, linewidth=2,
                 markersize=4, linestyle="--", alpha=0.7)
axes[1].set_title("Avg EC Transactions: Month N vs N+5")
axes[1].set_ylabel("Avg Transactions")
axes[1].legend(fontsize=8)
axes[1].grid(True, alpha=0.3)
axes[1].tick_params(axis="x", rotation=45)

# 3c: Avg disbursement now vs N+5
for label, color in [("Rent Payers", "#2563eb"), ("Non-Rent Payers", "#f97316")]:
    subset = retention[retention["label"] == label]
    axes[2].plot(subset["CURRENT_MONTH"], subset["avg_disb_now"],
                 marker="o", label=f"{label} (Month N)", color=color, linewidth=2, markersize=4)
    axes[2].plot(subset["CURRENT_MONTH"], subset["avg_disb_n5"],
                 marker="s", label=f"{label} (Month N+5)", color=color, linewidth=2,
                 markersize=4, linestyle="--", alpha=0.7)
axes[2].set_title("Avg EC Disbursement: Month N vs N+5")
axes[2].set_ylabel("Avg Disbursement ($)")
axes[2].legend(fontsize=8)
axes[2].grid(True, alpha=0.3)
axes[2].tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("ec_retention_n5.png", dpi=150)
print("  -> ec_retention_n5.png")

print("\nAll charts generated.")
