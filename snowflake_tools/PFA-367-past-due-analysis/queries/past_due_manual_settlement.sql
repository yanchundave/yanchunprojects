-- PFA-367: Past due users who settle manually, bucketed by past-due days
-- Logic:
--   1. Get all completed overdrafts with their disbursement month and due date
--   2. For each overdraft, find the last successful settlement date and whether any settlement was manual
--   3. An overdraft is "past due" if the last settlement date > due date (or still unsettled and due date has passed)
--   4. Among past-due overdrafts, calculate % with manual settlement
--   5. Bucket by days past due: 0-30, 30-60, 60-180, 180-360, 360+

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
    -- For each overdraft: last settlement date, total settled, and whether any manual settlement exists
    SELECT
        OVERDRAFT_ID,
        MAX(DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', COMPLETED_AT))) AS last_settlement_ds,
        SUM(AMOUNT) AS total_settled,
        MAX(CASE WHEN TRIGGER_TYPE = 'MANUAL' THEN 1 ELSE 0 END) AS has_manual_settlement,
        COUNT(*) AS settlement_count
    FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT
    WHERE SETTLEMENT_STATUS_ID = 2  -- successful settlements only
    GROUP BY 1
),

overdraft_with_settlement AS (
    SELECT
        o.OVERDRAFT_ID,
        o.USER_ID,
        o.disb_month,
        o.DISBURSEMENT_DS_PST,
        o.SETTLEMENT_DUE_DS_PST,
        o.AMOUNT_DUE,
        s.last_settlement_ds,
        s.total_settled,
        s.has_manual_settlement,
        s.settlement_count,
        -- Determine if past due: last settlement after due date, OR still unsettled and due date passed
        CASE
            WHEN s.last_settlement_ds IS NOT NULL AND s.last_settlement_ds > o.SETTLEMENT_DUE_DS_PST THEN 1
            WHEN s.last_settlement_ds IS NULL AND CURRENT_DATE > o.SETTLEMENT_DUE_DS_PST THEN 1
            ELSE 0
        END AS is_past_due,
        -- Days past due (from due date to last settlement, or to today if unsettled)
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

bucketed AS (
    SELECT
        *,
        CASE
            WHEN days_past_due BETWEEN 1 AND 30 THEN '01: 0-30 days'
            WHEN days_past_due BETWEEN 31 AND 60 THEN '02: 30-60 days'
            WHEN days_past_due BETWEEN 61 AND 180 THEN '03: 60-180 days'
            WHEN days_past_due BETWEEN 181 AND 360 THEN '04: 180-360 days'
            WHEN days_past_due > 360 THEN '05: 360+ days'
            ELSE '00: Not past due'
        END AS past_due_bucket
    FROM overdraft_with_settlement
)

-- Result 1: Monthly overview
SELECT
    disb_month,
    COUNT(DISTINCT OVERDRAFT_ID) AS total_overdrafts,
    COUNT(DISTINCT USER_ID) AS total_users,
    COUNT(DISTINCT CASE WHEN is_past_due = 1 THEN OVERDRAFT_ID END) AS past_due_overdrafts,
    COUNT(DISTINCT CASE WHEN is_past_due = 1 THEN USER_ID END) AS past_due_users,
    ROUND(past_due_overdrafts * 100.0 / NULLIF(total_overdrafts, 0), 2) AS pct_past_due_overdrafts,
    ROUND(past_due_users * 100.0 / NULLIF(total_users, 0), 2) AS pct_past_due_users,
    COUNT(DISTINCT CASE WHEN is_past_due = 1 AND has_manual_settlement = 1 THEN OVERDRAFT_ID END) AS past_due_manual_overdrafts,
    COUNT(DISTINCT CASE WHEN is_past_due = 1 AND has_manual_settlement = 1 THEN USER_ID END) AS past_due_manual_users,
    ROUND(past_due_manual_overdrafts * 100.0 / NULLIF(past_due_overdrafts, 0), 2) AS pct_manual_among_past_due_overdrafts,
    ROUND(past_due_manual_users * 100.0 / NULLIF(past_due_users, 0), 2) AS pct_manual_among_past_due_users
FROM bucketed
GROUP BY 1
ORDER BY 1;
