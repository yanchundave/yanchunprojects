# PFA-367: Past Due & Manual Settlement Analysis

**Jira:** [PFA-367](https://demoforthedaves.atlassian.net/browse/PFA-367)
**Wiki:** [Manually Settlement Rate](https://demoforthedaves.atlassian.net/wiki/spaces/D/pages/4516380737)
**Data Range:** Jan 2024 – Jan 2026 (Feb 2026 excluded as incomplete)
**Last Updated:** March 2026

---

## Folder Structure

```
PFA-367-past-due-analysis/
├── README.md              ← This file
├── data/
│   ├── past_due_monthly.csv    ← 26 rows: monthly aggregated metrics
│   └── past_due_buckets.csv    ← 110 rows: metrics by past-due bucket
├── queries/
│   ├── past_due_manual_settlement.sql  ← Monthly overview query
│   └── past_due_by_bucket.sql          ← Bucket breakdown query
├── charts/
│   ├── chart1_users_vs_pastdue.png     ← Total users vs past due (dual axis)
│   ├── chart2_pastdue_vs_manual.png    ← Past due vs manual settlement (dual axis)
│   ├── chart3_bucket_share.png         ← Bucket share of manual settlements (5 lines)
│   ├── chart4_pastdue_composition.png  ← Stacked area: past due by bucket
│   ├── chart5_bucket_detail.png        ← Bar charts: rate & volume by bucket
│   ├── past_due_rates.png              ← Earlier: line charts of rates
│   ├── past_due_buckets.png            ← Earlier: bucket bar charts
│   └── past_due_bucket_trend.png       ← Earlier: bucket trend lines
└── scripts/
    ├── viz_past_due_v2.py       ← Latest chart generator (chart1-5)
    ├── viz_past_due.py          ← Earlier chart generator
    ├── viz_bucket_trend.py      ← Bucket trend chart
    └── update_wiki.py           ← Confluence wiki updater
```

## Definitions

- **Past Due:** An overdraft whose last successful settlement occurred after the settlement due date, or remains unsettled with the due date already passed
- **Manual Settlement:** `TRIGGER_TYPE = 'MANUAL'` and `SETTLEMENT_STATUS_ID = 2` (successful) in `OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT`
- **Buckets:** 0-30, 30-60, 60-180, 180-360, 360+ days past due (days between due date and last settlement, or current date if unsettled)

## Data Sources

- `ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_DISBURSEMENT` — overdraft disbursements with due dates
- `OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT` — settlement records with trigger type and status

## Key Findings

| Metric | Value |
|--------|-------|
| Total Overdraft Users (Jan 2026) | 1,735,418 |
| Past Due Users (Jan 2026) | 766,081 (44.1%) |
| Manual Settlement Among Past Due | 135,814 (17.7%) |
| Past Due Rate Trend | 38.5% (2024) → 42.6% (2025), +4.0pp |
| Manual Rate Stability | Mean 17.5%, Std Dev 0.79pp |
| 0-30 Day Bucket Share | 91.2% of all manual settlements |
| December Seasonality | +4.6pp higher past due rate vs other months |
| Past Due User Growth | +74% (441K → 766K, Jan 2024 → Jan 2026) |

## How to Regenerate Charts

```bash
cd /Users/yanchunyang/Documents/claude_code/snowflake_tools
source ../venv/bin/activate
python viz_past_due_v2.py
```

## How to Re-query Data

Use the SQL files in `queries/` with the Snowflake CLI tool:
```bash
python ../snowflake_download.py -f queries/past_due_manual_settlement.sql -o data/past_due_monthly.csv
python ../snowflake_download.py -f queries/past_due_by_bucket.sql -o data/past_due_buckets.csv
```
