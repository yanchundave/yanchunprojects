# Example: Rent Data Analysis Project

This documents the complete rent analysis project as a reference for future analyses.

## Objective
Understand how many MTM users pay rent monthly, which competitors they use, how their ExtraCash usage compares to non-rent payers, and their retention at N+5 months.

## What We Did

### Phase 1: Explore (Mode Dashboard)
- **Dashboard:** DA.FDI.Main / "Plaid - Rent data analysis"
- Read 15 SQL queries from `~/Documents/mode/Mode/dave_saves/spaces/DA_FDI_Main/`
- Drew ER diagram of 7 tables
- Identified key concepts: MTM users, rent identification via Plaid category, competitor classification

### Phase 2: Design Query
- Created `mtm_rent_extracash.sql` with 4 CTEs:
  - `mtm_users` — from ONE_DAVE_TRANSACTING_USERS
  - `rent_transactions` — from BANKTRANSACTION with Plaid category filter
  - `rent_user_month` — aggregated by user+month with competitor classification
  - `ec_monthly` — from FCT_OVERDRAFT_DISBURSEMENT
- Key gotcha: `DISBURSEMENT_STATUS = 'COMPLETE'` not `'COMPLETED'`

### Phase 3: Download Data
- Downloaded 26.7M rows, 1.1GB CSV
- Command: `../venv/bin/python snowflake_download.py -f mtm_rent_extracash.sql -p snowflake-data-analytics -o mtm_rent_extracash_results.csv`
- Ran in background (~10 min)

### Phase 4: Visualizations
Created 5 charts:

1. **rent_rate_monthly.png** — ~11% stable rent rate (viz_rent_rate.py)
2. **competitor_breakdown.png** — 97% Other, 2.3% Bilt, 0.5% Rent App (viz_all.py)
3. **ec_comparison.png** — Rent payers: higher EC adoption (98.5% vs 95.1%), +30% disbursement (viz_all.py)
4. **ec_retention_n5.png** — Rent payers retain better at N+5: 65% vs 57% (viz_all.py)
5. **forecast_rent_users.png** — Holt-Winters forecast: 200K rent payers, 1.66M non-rent Jan 2026 (viz_forecast.py)

### Phase 5: Reports
- **HTML report:** `rent_analysis_report.html` with base64-embedded charts
- **Confluence wiki:** https://demoforthedaves.atlassian.net/wiki/spaces/~893611306/pages/4513038381
  - Uploaded 5 PNG attachments via REST API
  - Embedded using `<ac:image>` / `<ri:attachment>` tags

### Key Findings
1. ~11% of MTM users pay rent — stable across 17 months
2. 97% pay rent directly, Bilt leads competitors at 2.3%
3. Rent payers are more engaged EC users (+30% disbursement, +3.4pp adoption)
4. Rent payers retain better at N+5 (65% vs 57%, 8pp gap)
5. Forecast: gradual growth continuing in both segments

### Lessons Learned
- Always verify enum values in Snowflake (COMPLETE vs COMPLETED)
- Download data locally when multiple charts needed from same dataset
- Use base64-embedded HTML for self-contained local reports
- Upload Confluence attachments via REST API, then embed with `<ri:attachment>` in page body
- Confluence MCP tools don't support attachment upload — use curl with API token
