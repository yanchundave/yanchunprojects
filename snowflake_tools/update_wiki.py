#!/usr/bin/env python3
"""Update Confluence wiki page with PFA-367 analysis results."""

import os
import requests
import json

BASE = "https://demoforthedaves.atlassian.net/wiki"
PAGE_ID = "4516380737"
EMAIL = os.environ.get("CONFLUENCE_EMAIL", "your-email@example.com")
TOKEN = os.environ.get("CONFLUENCE_API_TOKEN", "your-api-token")
AUTH = (EMAIL, TOKEN)

# Get current version
resp = requests.get(
    f"{BASE}/api/v2/pages/{PAGE_ID}",
    auth=AUTH,
    headers={"Accept": "application/json"}
)
print(f"GET status: {resp.status_code}")
page = resp.json()
current_version = page["version"]["number"]
print(f"Current version: {current_version}")

# Build ADF body with images and insights
def text_node(t, marks=None):
    node = {"type": "text", "text": t}
    if marks:
        node["marks"] = marks
    return node

def heading(level, t):
    return {"type": "heading", "attrs": {"level": level}, "content": [text_node(t)]}

def paragraph(*nodes):
    return {"type": "paragraph", "content": list(nodes)}

def bold_text(t):
    return text_node(t, [{"type": "strong"}])

ATTACHMENT_IDS = {
    "chart1_users_vs_pastdue.png": "6123edfd-f1eb-4739-8880-d502bd3fccd3",
    "chart2_pastdue_vs_manual.png": "3fa1ef9c-cc6d-4b97-ac87-08db528eb19f",
    "chart3_bucket_share.png": "1a656a58-5ef1-42df-bf1b-706f236dce6a",
    "chart4_pastdue_composition.png": "5c2cce7e-dd44-4676-8ade-b644f31c71fc",
    "chart5_bucket_detail.png": "1017c9ae-11a6-4e2f-8d8d-a168d86bd362",
}

def media_single(filename):
    file_id = ATTACHMENT_IDS[filename]
    return {
        "type": "mediaSingle",
        "attrs": {"layout": "center", "width": 100},
        "content": [{
            "type": "media",
            "attrs": {
                "type": "file",
                "id": file_id,
                "collection": f"contentId-{PAGE_ID}",
                "width": 900,
                "height": 400
            }
        }]
    }

def hr():
    return {"type": "rule"}

def bullet_list(items):
    return {
        "type": "bulletList",
        "content": [{
            "type": "listItem",
            "content": [paragraph(text_node(item))]
        } for item in items]
    }

def table_row(cells, is_header=False):
    cell_type = "tableHeader" if is_header else "tableCell"
    return {
        "type": "tableRow",
        "content": [{
            "type": cell_type,
            "content": [paragraph(text_node(str(c)))]
        } for c in cells]
    }

body_content = [
    heading(1, "PFA-367: Past Due & Manual Settlement Analysis"),
    paragraph(
        bold_text("Jira: "),
        text_node("PFA-367  |  "),
        bold_text("Data Range: "),
        text_node("Jan 2024 – Jan 2026 (Feb 2026 excluded as incomplete)")
    ),
    paragraph(
        bold_text("Definition: "),
        text_node("An overdraft is \"past due\" if its last successful settlement occurred after the settlement due date, or it remains unsettled with the due date already passed. Manual settlement = TRIGGER_TYPE = 'MANUAL' in the settlement table.")
    ),
    hr(),

    # Chart 1
    heading(2, "1. Total Overdraft Users vs Past Due Users (Monthly)"),
    paragraph(text_node("Double Y-axis chart: bars show total overdraft users and past due users; line shows past due rate.")),
    media_single("chart1_users_vs_pastdue.png"),
    paragraph(bold_text("Key Observations:")),
    bullet_list([
        "Total overdraft users grew from 1.14M (Jan 2024) to 1.74M (Jan 2026) — 53% increase",
        "Past due users grew from 441K to 766K — 74% increase, outpacing total user growth",
        "Past due rate rose from 38.8% to 44.1% over the period",
        "December consistently shows the highest past due rates (~42-47%)"
    ]),
    hr(),

    # Chart 2
    heading(2, "2. Past Due Users vs Manual Settlement Users (Monthly)"),
    paragraph(text_node("Double Y-axis chart: bars show past due users and manual settlement users; line shows manual settlement rate.")),
    media_single("chart2_pastdue_vs_manual.png"),
    paragraph(bold_text("Key Observations:")),
    bullet_list([
        "Manual settlement users closely track past due users",
        "Manual settlement rate is remarkably stable: mean 17.5%, std dev only 0.79pp",
        "Rate consistently falls between 15.6% and 18.7% across all months",
        "No clear trend — the proportion settling manually is structural"
    ]),
    hr(),

    # Chart 3
    heading(2, "3. Distribution of Manual Settlements Across Past Due Buckets"),
    paragraph(text_node("Five lines showing each bucket's share of total manual settlement users (each month sums to 100%).")),
    media_single("chart3_bucket_share.png"),
    paragraph(bold_text("Key Observations:")),
    bullet_list([
        "0-30 day bucket dominates: 85-95% of all manual settlements",
        "30-60 day bucket is a distant second at 3-5%",
        "60-180 and 180-360 day buckets each contribute 1-4%",
        "360+ day bucket is negligible (<1%)"
    ]),
    hr(),

    # Chart 4
    heading(2, "4. Composition of Past Due Users by Bucket (Stacked Area)"),
    paragraph(text_node("Stacked area chart showing the proportion of past due users in each bucket over time.")),
    media_single("chart4_pastdue_composition.png"),
    paragraph(bold_text("Key Observations:")),
    bullet_list([
        "0-30 day bucket consistently represents 80-90% of all past due users",
        "Longer-duration buckets (180-360, 360+) have been shrinking as a share",
        "Most past due situations resolve relatively quickly"
    ]),
    hr(),

    # Chart 5
    heading(2, "5. Bucket Detail: Manual Settlement Rate & Volume Distribution"),
    paragraph(text_node("Left: Manual settlement rate by bucket. Right: Volume distribution of past due users across buckets.")),
    media_single("chart5_bucket_detail.png"),
    hr(),

    # Insights
    heading(2, "Key Insights"),

    heading(3, "1. Past Due Rate is Trending Up"),
    bullet_list([
        "2024 average: 38.5% of overdraft users are past due",
        "2025 average: 42.6% of overdraft users are past due",
        "+4.0 percentage point increase year-over-year",
        "Driven by faster growth in past due users relative to total users"
    ]),

    heading(3, "2. Strong December Seasonality"),
    bullet_list([
        "December past due rates average +4.6pp higher than non-December months",
        "Dec 2025 hit 47.2% — nearly half of all overdraft users",
        "Holiday spending likely contributes to delayed repayment"
    ]),

    heading(3, "3. Manual Settlement Rate is Structural (~17.5%)"),
    bullet_list([
        "Despite massive user growth, the manual settlement rate stays remarkably stable",
        "Mean: 17.5%, Std Dev: 0.79pp, CV: 4.5%",
        "Suggests manual settlement is a consistent behavioral pattern, not driven by external factors"
    ]),

    heading(3, "4. 0-30 Day Bucket Dominates Manual Settlements"),
    bullet_list([
        "91.2% of all manual settlements come from the 0-30 day past due bucket",
        "Vast majority of manual settlers act within the first 30 days",
        "Longer past due users rarely settle manually — they get auto-settled or remain delinquent"
    ]),

    heading(3, "5. Past Due User Growth Outpaces Total User Growth"),
    bullet_list([
        "Total users: +53% (1.14M → 1.74M)",
        "Past due users: +74% (441K → 766K)",
        "Gap suggests changing user quality or product dynamics increasing past due likelihood"
    ]),

    heading(3, "6. Manual Settlement Rate Varies by Bucket Duration"),
    bullet_list([
        "0-30 days: ~17-20% manual rate",
        "30-60 days: ~14-17% manual rate",
        "60-180 days: ~5-18% (declining trend in 2025)",
        "180-360 days: ~1-19% (highly variable, declining sharply in 2025)",
        "360+ days: <2% (essentially no manual settlement)"
    ]),
    hr(),

    # Summary table
    heading(2, "Data Summary (Jan 2026 — Latest Complete Month)"),
    {
        "type": "table",
        "attrs": {"isNumberColumnEnabled": False, "layout": "default"},
        "content": [
            table_row(["Metric", "Value"], is_header=True),
            table_row(["Total Overdraft Users", "1,735,418"]),
            table_row(["Past Due Users", "766,081 (44.1%)"]),
            table_row(["Manual Settlement Among Past Due", "135,814 (17.7%)"]),
        ]
    },
    hr(),

    # Methodology
    heading(2, "Methodology"),
    bullet_list([
        "Data Source: FCT_OVERDRAFT_DISBURSEMENT joined with OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT",
        "Past Due: Last successful settlement date > settlement due date, or unsettled with due date passed",
        "Manual Settlement: TRIGGER_TYPE = 'MANUAL' and SETTLEMENT_STATUS_ID = 2 (successful)",
        "Buckets: 0-30, 30-60, 60-180, 180-360, 360+ days past due",
        "Exclusions: Feb 2026 excluded (incomplete month)"
    ]),
]

adf = {
    "version": 1,
    "type": "doc",
    "content": body_content
}

# Update page
update_payload = {
    "id": PAGE_ID,
    "status": "current",
    "title": "Manually Settlement Rate",
    "body": {
        "representation": "atlas_doc_format",
        "value": json.dumps(adf)
    },
    "version": {
        "number": current_version + 1,
        "message": "Updated with 5 visualizations and insights"
    }
}

resp = requests.put(
    f"{BASE}/api/v2/pages/{PAGE_ID}",
    auth=AUTH,
    headers={"Content-Type": "application/json", "Accept": "application/json"},
    json=update_payload
)

print(f"PUT status: {resp.status_code}")
if resp.status_code == 200:
    print("Wiki page updated successfully!")
else:
    print(f"Error: {resp.text[:500]}")
