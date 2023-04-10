/*
    function to infer whether the transaction is to repay Advance to known competitors
    return a competitor name if yes, otherwise NULL
*/
CREATE OR REPLACE FUNCTION UDF_COMPETITOR_REPAY(DESCRIPTION VARCHAR, AMOUNT NUMBER)
    returns VARCHAR
as
$$
    -- Top competitors: Albert, Brigit, Empower, Earnin, Chime, Money Lion, Varo --
    CASE
         WHEN AMOUNT > 0  THEN NULL -- amount cannot be > $0
         WHEN LOWER(description) LIKE 'albert instant%' OR LOWER(description) LIKE 'albert savings%' THEN 'Albert'
         WHEN LOWER(description) LIKE '%brigit%' THEN 'Brigit'
         WHEN LOWER(description) LIKE '%empower%' THEN 'Empower'
         WHEN LOWER(description) LIKE '%earnin%' AND LOWER(description) NOT LIKE '%learnin%' THEN 'Earnin'
         WHEN LOWER(description) LIKE '%chime%' THEN 'Chime'
         WHEN LOWER(description) LIKE '%money%lion%' THEN 'Money Lion'
         WHEN LOWER(description) LIKE '%varo%' THEN 'Varo'
         ELSE NULL
    END
$$;

SET REQUEST_STARTING_DATE = '2022-04-01';

-- when do our competitors collect money? are we using the same strategy to collect on the upcoming paycheck days?
-- if yes, what is the success rate?

-- data: users who borrowed money from both Dave and at least 1 competitors
CREATE OR REPLACE TABLE DBT.DEV_HU_PUBLIC.advance_competitor_both_20220401_1w AS
WITH dave_advance AS (
    SELECT
        DISTINCT user_id
    FROM ANALYTIC_DB.DBT_metrics.credit_active_users
    WHERE transacting_ds_pst BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '6 days'
),

competitor_users AS (
    SELECT
        user_id,
        UDF_COMPETITOR(display_name, amount) AS competitor_name,
        MIN(transaction_date) AS borrow_date
        -- MAX(amount) AS amount -- assuming one user can get one advance from one competitor during a week
    FROM datastream_prd.dave.bank_transaction
    WHERE competitor_name IS NOT NULL -- borrow from competitors
        AND amount >= 1 -- positive and at least 1 dollar
        AND transaction_date BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '6 days'
    GROUP BY 1,2
)

SELECT
    competitor_users.*
FROM dave_advance
INNER JOIN competitor_users
    ON dave_advance.user_id = competitor_users.user_id
;


SELECT
    *
FROM DBT.DEV_HU_PUBLIC.advance_competitor_both_20220401_1w
LIMIT 10
;

-- 166,239
SELECT
    COUNT(*)
FROM DBT.DEV_HU_PUBLIC.advance_competitor_both_20220401_1w
;


WITH competitor_repay_first_transaction AS (
    SELECT
        user_id,
        UDF_COMPETITOR_REPAY(display_name, amount) AS competitor_name,
        transaction_date AS payment_date
        -- MAX(amount) AS amount -- assuming one user can get one advance from one competitor during a week
    FROM datastream_prd.dave.bank_transaction
    WHERE competitor_name IS NOT NULL -- repay to competitors
        AND amount < 0
        AND transaction_date BETWEEN DATE($REQUEST_STARTING_DATE) AND DATE($REQUEST_STARTING_DATE) + interval '30 days'
    -- GROUP BY 1,2
),

user_repay AS (
    SELECT
        u.user_id,
        u.competitor_name,
        MIN(p.payment_date) AS first_payment_date
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_both_20220401_1w u
    LEFT JOIN competitor_repay_first_transaction p ON u.user_id = p.user_id AND u.competitor_name = p.competitor_name AND u.borrow_date < p.payment_date
    GROUP BY 1,2
)

SELECT
    competitor_name,
    first_payment_date,
    COUNT(*) AS user_cnt
FROM user_repay
GROUP BY 1,2
ORDER BY 1,2
;


SELECT
    *
FROM analytic_db.dbt_marts.fct_advances
LIMIT 10
;


SELECT
    *
FROM analytic_db.dbt_marts.fct_overdraft_disbursement
LIMIT 10
;


WITH dave_advance AS (
    SELECT
        user_id,
        disbursement_ts_pst,
        payback_ds_pst AS due_date
    FROM analytic_db.dbt_marts.fct_advances
    WHERE disbursement_ts_pst BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '6 days'

    UNION

    SELECT
        user_id,
        disbursement_ts_pst,
        settlement_due_ds AS due_date
    FROM analytic_db.dbt_marts.fct_overdraft_disbursement
    WHERE disbursement_ts_pst BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '6 days'
),

borrowers AS (
    SELECT DISTINCT user_id
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_both_20220401_1w
)

SELECT
    dave_advance.due_date,
    COUNT(*) AS cnt
FROM borrowers
LEFT JOIN dave_advance ON borrowers.user_id = dave_advance.user_id
GROUP BY 1
ORDER BY 1
;