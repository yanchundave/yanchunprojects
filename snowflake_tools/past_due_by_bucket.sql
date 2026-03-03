-- PFA-367: Past due manual settlement breakdown by past-due bucket
-- Same CTEs as the monthly query, but grouped by bucket instead

WITH overdrafts AS (
    SELECT
        OVERDRAFT_ID,
        USER_ID,
        DATE_TRUNC('month', DISBURSEMENT_DS_PST) AS disb_month,
        DISBURSEMENT_DS_PST,
        SETTLEMENT_DUE_DS_PST,
        AMOUNT_DUE
    FROM ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_DISBURSEMENT
    WHERE DISBURSEMENT_STATUS = 'COMPLETE'
      AND DISBURSEMENT_DS_PST >= '2024-01-01'
      AND DISBURSEMENT_DS_PST < '2026-03-01'
),

settlement_agg AS (
    SELECT
        OVERDRAFT_ID,
        MAX(DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', COMPLETED_AT))) AS last_settlement_ds,
        SUM(AMOUNT) AS total_settled,
        MAX(CASE WHEN TRIGGER_TYPE = 'MANUAL' THEN 1 ELSE 0 END) AS has_manual_settlement,
        COUNT(*) AS settlement_count
    FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT
    WHERE SETTLEMENT_STATUS_ID = 2
    GROUP BY 1
),

overdraft_with_settlement AS (
    SELECT
        o.OVERDRAFT_ID,
        o.USER_ID,
        o.disb_month,
        o.SETTLEMENT_DUE_DS_PST,
        s.last_settlement_ds,
        s.has_manual_settlement,
        CASE
            WHEN s.last_settlement_ds IS NOT NULL AND s.last_settlement_ds > o.SETTLEMENT_DUE_DS_PST THEN 1
            WHEN s.last_settlement_ds IS NULL AND CURRENT_DATE > o.SETTLEMENT_DUE_DS_PST THEN 1
            ELSE 0
        END AS is_past_due,
        CASE
            WHEN s.last_settlement_ds IS NOT NULL AND s.last_settlement_ds > o.SETTLEMENT_DUE_DS_PST
                THEN DATEDIFF('day', o.SETTLEMENT_DUE_DS_PST, s.last_settlement_ds)
            WHEN s.last_settlement_ds IS NULL AND CURRENT_DATE > o.SETTLEMENT_DUE_DS_PST
                THEN DATEDIFF('day', o.SETTLEMENT_DUE_DS_PST, CURRENT_DATE)
            ELSE 0
        END AS days_past_due
    FROM overdrafts o
    LEFT JOIN settlement_agg s ON o.OVERDRAFT_ID = s.OVERDRAFT_ID
),

past_due_only AS (
    SELECT
        *,
        CASE
            WHEN days_past_due BETWEEN 1 AND 30 THEN '01: 0-30 days'
            WHEN days_past_due BETWEEN 31 AND 60 THEN '02: 30-60 days'
            WHEN days_past_due BETWEEN 61 AND 180 THEN '03: 60-180 days'
            WHEN days_past_due BETWEEN 181 AND 360 THEN '04: 180-360 days'
            WHEN days_past_due > 360 THEN '05: 360+ days'
        END AS past_due_bucket
    FROM overdraft_with_settlement
    WHERE is_past_due = 1
)

-- Result: Breakdown by past-due bucket and disbursement month
SELECT
    disb_month,
    past_due_bucket,
    COUNT(DISTINCT OVERDRAFT_ID) AS past_due_overdrafts,
    COUNT(DISTINCT CASE WHEN has_manual_settlement = 1 THEN OVERDRAFT_ID END) AS manual_settlement_overdrafts,
    ROUND(manual_settlement_overdrafts * 100.0 / NULLIF(past_due_overdrafts, 0), 2) AS pct_manual,
    COUNT(DISTINCT USER_ID) AS past_due_users,
    COUNT(DISTINCT CASE WHEN has_manual_settlement = 1 THEN USER_ID END) AS manual_settlement_users,
    ROUND(manual_settlement_users * 100.0 / NULLIF(past_due_users, 0), 2) AS pct_manual_users
FROM past_due_only
GROUP BY 1, 2
ORDER BY 1, 2;
