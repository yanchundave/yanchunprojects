WITH mtm_users AS (
    SELECT DISTINCT
        USER_ID,
        DATE_TRUNC('month', TRANSACTING_DS_PST) AS current_month
    FROM ANALYTIC_DB.DBT_METRICS.ONE_DAVE_TRANSACTING_USERS
    WHERE TRANSACTING_DS_PST >= '2024-08-01'
      AND TRANSACTING_DS_PST < '2026-01-01'
),

rent_transactions AS (
    SELECT
        t.DAVEUSERID AS user_id,
        DATE_TRUNC('month', t.TRANSACTIONDATE) AS rent_month,
        CASE
            WHEN cp.name ILIKE 'flex finance'
              OR t.EXTERNALNAME ILIKE 'flex finance%'
              OR t.EXTERNALNAME ILIKE '%getflex.com%'
                THEN 'Flex Finance'
            WHEN cp.name ILIKE 'bilt'
              OR (cp.name IS NULL AND t.EXTERNALNAME ILIKE 'bilt%')
                THEN 'Bilt'
            WHEN cp.name ILIKE '%rent app%'
              AND cp.name NOT ILIKE '%Rent Appeal%'
              AND cp.name NOT LIKE 'Rent Appli%'
                THEN 'Rent App'
            WHEN cp.name IN ('Lvble Repayment', 'Lvble')
              OR (cp.name IS NULL AND t.EXTERNALNAME ILIKE 'Lvble%')
                THEN 'Livable'
            WHEN cp.name ILIKE '%zillow%'
              OR t.EXTERNALNAME ILIKE '%zillow%'
                THEN 'Zillow'
            ELSE 'Other'
        END AS competitor,
        SUM(ABS(t.amount)) AS rent_paid
    FROM DAVE.BANK_DATA_SERVICE.BANKTRANSACTION t
    LEFT JOIN DAVE.BANK_DATA_SERVICE.TRANSACTIONCOUNTERPARTY tcp
        ON t.id = tcp.id
        AND t.bankaccountid = tcp.bankaccountid
        AND t.DAVEUSERID = tcp.daveuserid
    LEFT JOIN DAVE.BANK_DATA_SERVICE.COUNTERPARTY cp
        ON tcp.counterpartyid = cp.id
    WHERE t.TRANSACTIONDATE >= '2024-08-01'
      AND t.TRANSACTIONDATE < '2026-01-01'
      AND t.categorymetadata:"detailed"::VARCHAR = 'RENT_AND_UTILITIES_RENT'
      AND t.amount < 0
      AND t.DELETED IS NULL
    GROUP BY 1, 2, 3
),

rent_user_month AS (
    SELECT
        user_id,
        rent_month,
        SUM(rent_paid) AS rent_paid,
        MAX_BY(competitor, rent_paid) AS competitor
    FROM rent_transactions
    GROUP BY 1, 2
),

ec_monthly AS (
    SELECT
        USER_ID,
        DATE_TRUNC('month', DISBURSEMENT_DS_PST) AS ec_month,
        COUNT(DISTINCT OVERDRAFT_ID) AS extracash_transactions,
        SUM(OVERDRAFT_AMOUNT) AS extracash_disbursement
    FROM ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_DISBURSEMENT
    WHERE DISBURSEMENT_DS_PST >= '2024-08-01'
      AND DISBURSEMENT_DS_PST < '2026-06-01'
      AND DISBURSEMENT_STATUS = 'COMPLETE'
    GROUP BY 1, 2
)

SELECT
    m.user_id,
    m.current_month,
    CASE WHEN r.user_id IS NOT NULL THEN 1 ELSE 0 END AS if_paid_rent,
    COALESCE(r.rent_paid, 0) AS rent_paid,
    COALESCE(r.competitor, 'None') AS competitor,
    COALESCE(ec_cur.extracash_transactions, 0) AS extracash_transactions,
    COALESCE(ec_cur.extracash_disbursement, 0) AS extracash_disbursement,
    COALESCE(ec_fut.extracash_transactions, 0) AS after_5_month_extracash_transactions,
    COALESCE(ec_fut.extracash_disbursement, 0) AS after_5_month_disbursement
FROM mtm_users m
LEFT JOIN rent_user_month r
    ON m.user_id = r.user_id
    AND m.current_month = r.rent_month
LEFT JOIN ec_monthly ec_cur
    ON m.user_id = ec_cur.user_id
    AND m.current_month = ec_cur.ec_month
LEFT JOIN ec_monthly ec_fut
    ON m.user_id = ec_fut.user_id
    AND DATEADD('month', 5, m.current_month) = ec_fut.ec_month
ORDER BY m.user_id, m.current_month
