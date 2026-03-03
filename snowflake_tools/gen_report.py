#!/usr/bin/env python3
"""Generate HTML report with embedded charts for rent data analysis."""

import base64
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

def embed_img(filename):
    with open(os.path.join(script_dir, filename), "rb") as f:
        return base64.b64encode(f.read()).decode()

img_rent_rate = embed_img("rent_rate_monthly.png")
img_competitor = embed_img("competitor_breakdown.png")
img_ec = embed_img("ec_comparison.png")
img_retention = embed_img("ec_retention_n5.png")

html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Rent Data Analysis Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 1100px; margin: 40px auto; padding: 0 20px; color: #333; }}
  h1 {{ border-bottom: 2px solid #2563eb; padding-bottom: 8px; }}
  h2 {{ color: #1e40af; margin-top: 40px; }}
  h3 {{ color: #374151; margin-top: 24px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: right; }}
  th {{ background: #f1f5f9; text-align: center; }}
  td:first-child, th:first-child {{ text-align: left; }}
  img {{ max-width: 100%; border: 1px solid #e2e8f0; border-radius: 4px; margin: 12px 0; }}
  .finding {{ background: #f0f9ff; border-left: 4px solid #2563eb; padding: 12px 16px; margin: 12px 0; }}
  .meta {{ color: #64748b; font-size: 0.9em; }}
  .highlight {{ color: #dc2626; font-weight: bold; }}
</style>
</head>
<body>

<h1>Rent Data Analysis Report</h1>
<p class="meta">Generated: 2026-02-28 | Data: Aug 2024 &ndash; Dec 2025 | Source: Snowflake + Mode Dashboard (DA.FDI.Main)</p>

<!-- ========== Section 1: Rent Rate ========== -->
<h2>1. Monthly Rent Payer Rate Among MTM Users</h2>

<div class="finding">
<strong>Key Findings:</strong>
<ul>
  <li><strong>~11% of MTM users pay rent</strong> each month, remarkably stable over the 17-month period</li>
  <li>Rate ranges from <strong>10.7%</strong> (Nov 2025) to <strong>11.7%</strong> (Feb/Mar 2025)</li>
  <li>Total MTM users grew from <strong>1.42M</strong> (Aug 2024) to <strong>1.84M</strong> (Dec 2025), but the rent-paying share stayed flat</li>
</ul>
</div>

<img src="data:image/png;base64,{img_rent_rate}" alt="Monthly Rent Rate Chart">

<table>
<tr><th>Month</th><th>Total MTM Users</th><th>Rent Payers</th><th>Rent Rate</th></tr>
<tr><td>2024-08</td><td>1,421,007</td><td>163,343</td><td>11.49%</td></tr>
<tr><td>2024-09</td><td>1,436,262</td><td>159,597</td><td>11.11%</td></tr>
<tr><td>2024-10</td><td>1,465,144</td><td>164,271</td><td>11.21%</td></tr>
<tr><td>2024-11</td><td>1,461,712</td><td>162,007</td><td>11.08%</td></tr>
<tr><td>2024-12</td><td>1,511,794</td><td>169,038</td><td>11.18%</td></tr>
<tr><td>2025-01</td><td>1,493,993</td><td>172,723</td><td>11.56%</td></tr>
<tr><td>2025-02</td><td>1,435,911</td><td>168,637</td><td>11.74%</td></tr>
<tr><td>2025-03</td><td>1,424,552</td><td>167,103</td><td>11.73%</td></tr>
<tr><td>2025-04</td><td>1,498,145</td><td>171,647</td><td>11.46%</td></tr>
<tr><td>2025-05</td><td>1,561,753</td><td>182,305</td><td>11.67%</td></tr>
<tr><td>2025-06</td><td>1,583,166</td><td>180,005</td><td>11.37%</td></tr>
<tr><td>2025-07</td><td>1,659,312</td><td>189,824</td><td>11.44%</td></tr>
<tr><td>2025-08</td><td>1,710,550</td><td>193,005</td><td>11.28%</td></tr>
<tr><td>2025-09</td><td>1,718,941</td><td>192,512</td><td>11.20%</td></tr>
<tr><td>2025-10</td><td>1,762,916</td><td>202,192</td><td>11.47%</td></tr>
<tr><td>2025-11</td><td>1,765,030</td><td>188,974</td><td>10.71%</td></tr>
<tr><td>2025-12</td><td>1,837,059</td><td>199,288</td><td>10.85%</td></tr>
</table>

<!-- ========== Section 2: Competitor Breakdown ========== -->
<h2>2. Competitor Breakdown Among Rent Payers</h2>

<div class="finding">
<strong>Key Findings:</strong>
<ul>
  <li><strong>97% of rent payers pay directly</strong> ("Other") without using a third-party rent payment platform</li>
  <li><strong>Bilt</strong> is the leading competitor at <strong>2.3%</strong> share (~4,500 users in Dec 2025)</li>
  <li><strong>Rent App</strong> holds <strong>0.5%</strong> share (~1,000 users)</li>
  <li>Zillow (0.1%) and Flex Finance (~0%) have minimal presence</li>
  <li>Competitor shares have been stable over the entire period</li>
</ul>
</div>

<img src="data:image/png;base64,{img_competitor}" alt="Competitor Breakdown Chart">

<!-- ========== Section 3: EC Comparison ========== -->
<h2>3. ExtraCash Usage: Rent Payers vs Non-Rent Payers</h2>

<div class="finding">
<strong>Key Findings:</strong>
<ul>
  <li><strong>Rent payers have higher EC adoption</strong>: 98.5% vs 95.1% (Dec 2025)</li>
  <li><strong>Rent payers get larger advances</strong>: $537 avg disbursement vs $414 for non-rent payers (30% higher)</li>
  <li>Transaction counts are similar (~2.0 vs ~1.95 per month)</li>
  <li>Both groups show <strong>upward trends</strong> in adoption, transactions, and disbursement amounts over time</li>
  <li>The gap in disbursement amount has been <strong>widening</strong> &mdash; rent payers increasingly receive larger advances</li>
</ul>
</div>

<img src="data:image/png;base64,{img_ec}" alt="ExtraCash Comparison Chart">

<!-- ========== Section 4: N+5 Retention ========== -->
<h2>4. ExtraCash Retention at N+5 Months</h2>

<div class="finding">
<strong>Key Findings:</strong>
<ul>
  <li><strong>Significant drop in EC activity at N+5</strong>: adoption falls from ~97% to ~65% for rent payers, ~92% to ~57% for non-rent payers</li>
  <li><strong>Rent payers retain better</strong>: ~65% still active at N+5 vs ~57% for non-rent payers (8pp gap)</li>
  <li>Average transactions at N+5 drop to ~1.3&ndash;1.4 (from ~1.9&ndash;2.0), roughly a 30% decline</li>
  <li>Disbursement amounts at N+5 are approximately <strong>60&ndash;70% of Month N levels</strong></li>
  <li>Rent payers maintain a consistent advantage in retention across all metrics and time periods</li>
</ul>
</div>

<img src="data:image/png;base64,{img_retention}" alt="EC Retention N+5 Chart">

<!-- ========== Summary ========== -->
<h2>5. Summary &amp; Implications</h2>

<div class="finding">
<strong>Overall Takeaways:</strong>
<ul>
  <li><strong>Rent payers are more engaged ExtraCash users</strong> &mdash; higher adoption, larger advances, and better retention</li>
  <li>The ~11% rent-paying rate is stable, suggesting a <strong>consistent segment</strong> rather than a growing trend</li>
  <li>Third-party rent platforms (Bilt, Rent App) capture only ~3% of rent payers &mdash; <strong>minimal competitive pressure</strong> currently</li>
  <li>N+5 retention shows meaningful drop-off for both groups, but <strong>rent payers are stickier</strong>, making them a valuable cohort for retention strategies</li>
</ul>
</div>

</body>
</html>"""

output_path = os.path.join(script_dir, "rent_analysis_report.html")
with open(output_path, "w") as f:
    f.write(html)

print(f"Report saved to {output_path}")
