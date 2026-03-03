---
name: data-analysis
description: Run a data analysis project end-to-end. Use when starting a new analytical investigation — from understanding the objective, exploring Mode dashboards, designing SQL, downloading data, generating visualizations, and publishing results to Confluence.
argument-hint: [objective or Jira ticket]
allowed-tools: Read, Glob, Grep, Write, Edit, Bash, Task
---

# Data Analysis Project Skill

You are running a structured data analysis project. Follow the phases below in order. Each phase has a checkpoint — pause and confirm with the user before proceeding to the next phase.

## Input

The user will provide one of:
- A plain-text objective (e.g., "understand how many users pay rent monthly")
- A Jira ticket key (e.g., "DA-123") — fetch and read the ticket description
- Arguments: $ARGUMENTS

## Phase 1: Understand the Objective

1. Clarify the business question with the user
2. Ask: Is there a related **Mode dashboard**? If yes, get the collection name and dashboard name
3. If a Mode dashboard is provided, read the SQL queries from `~/Documents/mode/<collection>/<dashboard>/spaces/`
4. Based on the queries, draw an **ER diagram** of the tables and their relationships
5. Document key concepts (definitions of user segments, metrics, etc.)

**CHECKPOINT:** Present findings and wait for user input. The user may:
- Provide additional table names to explore
- Correct any mistakes in the ER diagram
- Explain the specific analysis they want

## Phase 2: Design the Query

1. If the user provides new table names, use `snowflake_download.py` to download their schemas:
   ```bash
   ../venv/bin/python snowflake_download.py -q "DESCRIBE TABLE <table_name>" -p snowflake-data-analytics -o schema_<table>.csv
   ```
2. Update the ER diagram with new tables
3. Draft the SQL query to answer the business question
4. Show the query to the user for review

**CHECKPOINT:** Wait for user approval before running the query.

## Phase 3: Get the Data

Decide whether to download data locally based on the situation:

**Download to local CSV when:**
- Multiple charts/visualizations are needed from the same data
- Iterative analysis (explore, then decide next steps)
- Data will be reused across multiple analysis steps
- Forecasting or statistical modeling is needed

**Query Snowflake directly (no download) when:**
- Simple one-off aggregation (e.g., a count or summary table)
- Result is small (< 1,000 rows) and only needed once
- Just checking a value or validating data

To download:
```bash
../venv/bin/python snowflake_download.py -f <query>.sql -p snowflake-data-analytics -o <output>.csv
```

**Important notes:**
- Working directory: `~/Documents/claude_code/snowflake_tools/`
- Python venv: `../venv/bin/python`
- profiles.yml must exist in the snowflake_tools/ directory
- `DISBURSEMENT_STATUS` in `FCT_OVERDRAFT_DISBURSEMENT` uses value `'COMPLETE'` (not `'COMPLETED'`)
- For large queries (>10M rows), run in background with timeout 600000

## Phase 4: Analyze and Visualize

1. Load the CSV with pandas and verify columns have data (watch for all-zeros — indicates a query bug)
2. Create visualizations using matplotlib:
   - Use clear titles, axis labels, legends
   - Use consistent color scheme: `#2563eb` (blue), `#f97316` (orange), `#dc2626` (red), `#16a34a` (green)
   - Save as PNG at 150 DPI
3. Print summary statistics to the console
4. If the user requests forecasting, use `statsmodels` (Holt-Winters or ARIMA)
   - Show forecast values as red diamonds with dashed lines
   - Label predicted values on the chart
5. Show charts to the user for feedback

## Phase 5: Generate Reports

### HTML Report (always)
Generate a self-contained HTML report with base64-embedded chart images:
- Use `gen_report.py` pattern: read PNGs as base64, embed in `<img>` tags
- Include key findings as bullet points above each chart
- Include summary tables with the actual numbers
- Save as `<analysis_name>_report.html`

### Confluence Wiki (if requested)
1. Upload chart images via REST API:
   ```bash
   curl -s -u "<email>:<api_token>" -X PUT -H "X-Atlassian-Token: nocheck" \
     -F "file=@<image>.png" \
     "https://demoforthedaves.atlassian.net/wiki/rest/api/content/<pageId>/child/attachment"
   ```
2. Extract the `fileId` from the upload response
3. Update the page content using MCP tools or REST API, embedding images with:
   ```html
   <ac:image ac:align="center" ac:layout="center"><ri:attachment ri:filename="<image>.png" ri:version-at-save="1" /></ac:image>
   ```

**Confluence details:**
- Cloud ID: `50601d82-2bc5-4e44-b497-fa77a5032c7d`
- API auth: User must provide email + API token (or reuse from memory)

## Phase 6: Wrap Up

1. Update the Confluence wiki page with all findings, charts, and summary
2. List all generated files for the user
3. Suggest follow-up analyses if relevant

---

## Reference: Project Structure

```
~/Documents/claude_code/
├── venv/                          # Python virtual environment
├── analysis_template.md           # Analysis workflow template
├── CLAUDE.md                      # Project instructions
├── .gitignore
└── snowflake_tools/
    ├── snowflake_download.py      # Snowflake CLI tool
    ├── profiles.yml               # Snowflake credentials (gitignored)
    ├── requirements.txt           # Python dependencies
    └── <analysis files>           # SQL, CSV, PNG, HTML per project
```

## Reference: Installed Python Packages

Available in `../venv/bin/python`:
- pandas, numpy, matplotlib (visualization)
- snowflake-connector-python (data download)
- statsmodels, scipy (forecasting / statistics)
- pyyaml, cryptography (Snowflake auth)
