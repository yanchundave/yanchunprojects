# How to Communicate Effectively with Claude for Data Analysis

*Based on lessons learned from PFA-367 Past Due & Manual Settlement Analysis*

---

## What Works Well

Strong context-setting in requests, including:
- **The goal** ("% of past due users settle manually")
- **Data sources** (fct_overdraft_disbursement, SETTLEMENT table)
- **Domain knowledge** ("advance, extra cash, and overdraft are basically same thing")
- **Where to find schema** (dbt repo path, Snowflake directly)

---

## Recommended Request Template

```
## Objective
What business question are you trying to answer? (1-2 sentences)

## Data Sources
- Table/model names
- Where to find schemas (dbt repo, Snowflake, etc.)
- Key columns or joins you already know about

## Metrics Needed
- List each metric clearly
- Specify numerator/denominator for rates
- Specify granularity (monthly, weekly, by user, by transaction)

## Visualization Preferences (if any)
- Chart types (bar, line, dual-axis, etc.)
- Axes definitions
- Any grouping/bucketing logic

## Output Destinations
- Jira ticket (link)
- Confluence page (link)
- Local file only?

## Constraints
- Date range
- Filters (e.g., only completed disbursements)
- Known gotchas (e.g., "status is 'COMPLETE' not 'COMPLETED'")
```

---

## Specific Tips

### 1. Specify Filters Early
We lost a round-trip when Claude defaulted to `< 2026-01-01` and the user asked "why not include all data?" If you have a preferred date range or know the latest complete month, mention it upfront:
> "Include all data through Jan 2026. Feb 2026 is incomplete — exclude it."

### 2. Describe Charts Precisely
Good example (saved a revision cycle):
> "Double Y axis — bars for total users and past due users, line for rate, label the rate on the line"

Less effective:
> "past due users / total users"

The more specific version saved an entire revision cycle.

### 3. Share Domain Knowledge Early
Notes like "advance, extra cash, and overdraft are basically the same thing" prevent confusion. Similarly, share:
- Which status values are valid
- How tables join together
- Business logic quirks

Drop them in early, even if they seem obvious.

### 4. Tell Claude the Audience
Mentioning "this is for a stakeholder presentation" early changes how output is packaged:
- Organized file structure from the start
- README and documentation written sooner
- Chart titles made more presentation-ready

### 5. Batch Related Requests
Instead of sequential asks:
1. "Draw the graphs" → then later
2. "Update Jira" → then later
3. "Update wiki" → then later
4. "Add more charts and insights"

Batch upfront:
> "Generate charts, post to Jira PFA-367, update the wiki page [link], and include insights."

This lets Claude plan the full workflow in one pass.

### 6. Review SQL Before Execution
Always review SQL before Claude runs it — especially for:
- Filter logic
- Join conditions
- Metric definitions (numerator/denominator)

This catches issues like wrong date filters or incorrect status values early.

---

## Example: Well-Structured Request

> **Objective:** Understand what % of past-due overdraft users settle manually, broken down by days-past-due buckets.
>
> **Data Sources:**
> - `ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_DISBURSEMENT` (dbt definition at ~/github/dave-dbt/dave-sql)
> - `OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT` (check schema via Snowflake)
> - Reference: `fct_extracash_collection` for how settlement table is used
>
> **Metrics:**
> - Monthly total overdraft users, past-due users, past-due rate
> - Manual settlement users among past-due, manual rate
> - Breakdown by bucket: 0-30, 30-60, 60-180, 180-360, 360+ days
> - Past due = last successful settlement after due date, or unsettled past due date
>
> **Visualizations:**
> - Dual-axis bar+line: total users vs past-due users with rate line (labeled)
> - Dual-axis bar+line: past-due vs manual users with rate line (labeled)
> - Five-line chart: each bucket's share of manual settlements (sums to 100%)
> - Any additional charts with insights
>
> **Output:** Jira PFA-367, wiki page [link], save all artifacts locally for stakeholder follow-up
>
> **Constraints:** Include through Jan 2026. Feb 2026 is incomplete. Status filter is 'COMPLETE' not 'COMPLETED'.

---

## Summary

**Be specific about metrics, charts, and destinations upfront, and share domain gotchas early.** A bit more structure saves round-trips and gets to the final result faster.
