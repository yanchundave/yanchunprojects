-- https://demoforthedaves.atlassian.net/browse/DAT-749
-- for the approved but not taken users, do they take competitor offer?
-- If yes, compare amount (and fee if possible)?
-- Also, is it because they have multiple recurring incomes and we offer lower amount

SET REQUEST_STARTING_DATE = '2022-04-01';

//SET REQUEST_END_DATE = '2022-04-07';
SELECT DATE($REQUEST_STARTING_DATE) + interval '6 days';

SELECT DATE($REQUEST_STARTING_DATE) + interval '13 days';

//2022-04-08	2022-04-14
SELECT DATE($REQUEST_STARTING_DATE) + interval '7 days', DATE($REQUEST_STARTING_DATE) + interval '13 days';

-- 2022-04-15	2022-04-30
SELECT DATE($REQUEST_STARTING_DATE) + interval '14 days', DATE($REQUEST_STARTING_DATE) + interval '29 days';

-- SELECT *
-- FROM analytic_db.dbt_marts.fct_advance_approvals
-- LIMIT 10
-- ;

-- SELECT
--     created,
--     transaction_date,
--     user_id,
--     bank_account_id,
--     UDF_IS_COMPETITOR(display_name),
--     display_name,
--     amount,
--     *
-- -- FROM APPLICATION_DB.BANK_TRANSACTIONS_CSV.BANK_TRANSACTION_DATA_FROM_CSV
-- FROM datastream_prd.dave.bank_transaction
-- WHERE UDF_IS_COMPETITOR(display_name) = true -- borrow from competitors
--     AND amount > 0 -- positive
-- LIMIT 10
-- ;

-- -- Varo Advance
-- -- Transfer From Varo Believe Secured Account To Bank
-- -- NO: Transfer From Varo Savings Account To Bank
-- SELECT
--     display_name,
--     external_name,
--     amount,
--     created,
--     transaction_date,
--     user_id,
--     bank_account_id,
--     *
-- -- FROM APPLICATION_DB.BANK_TRANSACTIONS_CSV.BANK_TRANSACTION_DATA_FROM_CSV
-- FROM datastream_prd.dave.bank_transaction
-- WHERE LOWER(display_name) LIKE '%varo%'  -- borrow from competitors
--     AND amount > 1 -- positive
--     AND transaction_date > '2022-04-01'
-- LIMIT 100
-- ;

-- -- Transfer From Varo Savings Account To Bank	21,497,608.77	375,234
-- -- Transfer From Varo Bank Account To Savings	15,610,774.87	93,093
-- -- Transfer From Varo Believe Secured Account To Bank	2,501,208.96	41,250
-- -- Varo Advance	923,315	18,161
-- -- Transfer From Varo Savings Account To Checking	100,546.28	1,924
-- -- Varo bank na, varotrsfr ,	237,804.6	1,814
-- SELECT
--     display_name,
--     SUM(amount),
--     COUNT(*)
-- -- FROM APPLICATION_DB.BANK_TRANSACTIONS_CSV.BANK_TRANSACTION_DATA_FROM_CSV
-- FROM datastream_prd.dave.bank_transaction
-- WHERE LOWER(display_name) LIKE '%varo%'  -- borrow from competitors
--     AND amount > 1 -- positive
--     AND transaction_date = '2022-04-01'
-- GROUP BY 1
-- ORDER BY 3 DESC
-- ;

SELECT
        *
FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests
LIMIT 10
;



CREATE OR REPLACE FUNCTION UDF_COMPETITOR(DESCRIPTION VARCHAR, AMOUNT NUMBER)
    returns VARCHAR
as
$$
    -- Top competitors: Albert, Brigit, Empower, Earnin, Chime, Money Lion, Varo --
    CASE
         WHEN AMOUNT < 1 OR AMOUNT > 250  THEN NULL -- amount cannot be < $1 or > $250
         WHEN AMOUNT % 5 > 0 THEN NULL -- amount cannot be divided by 5
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


CREATE OR REPLACE FUNCTION UDF_OFFER_BUCKET(AMOUNT NUMBER)
    returns VARCHAR
as
$$
    CASE
        WHEN AMOUNT IS NULL THEN NULL
        WHEN AMOUNT < 25 THEN '1. < $25'
        WHEN AMOUNT < 50 THEN '2. [$25, 50)'
        WHEN AMOUNT < 75 THEN '3. [$50, $75)'
        WHEN AMOUNT < 100 THEN '4. [$75, $100)'
        WHEN AMOUNT < 150 THEN '5. [$100, $150)'
        ELSE '6. >=150'
   END
$$;


CREATE OR REPLACE TABLE DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w AS
WITH dave_advance AS (
    -- approved and never taken in the week
    SELECT
        -- advance_requests.requested_ds,
        advance_requests.user_id,
        -- advance_requests.bank_account_id,
        MAX(IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL, 1, 0)) AS is_approved_in_the_week,
        MAX(COALESCE(advance_requests.max_approved_amount, 0)) AS max_approved_amount,
        MAX(IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0)) AS has_taken_in_the_week
    FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
        ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
        ON advance_requests.advance_approval_id = o2_takeout.approval_id
    WHERE advance_requests.requested_ds BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '13 days'
        -- AND TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL -- approved
    GROUP BY 1
    -- HAVING has_taken_in_the_week = 0 -- not takeout in the week
),

competitor_transactions AS (
    SELECT
        -- transaction_date,
        user_id,
        -- bank_account_id,
        display_name,
        UDF_COMPETITOR(display_name, amount) AS competitor_name,
        MAX(amount) AS amount -- assuming one user can get one advance from one competitor during a week
    FROM datastream_prd.dave.bank_transaction
    WHERE competitor_name IS NOT NULL -- borrow from competitors
        AND amount >= 1 -- positive and at least 1 dollar
        AND transaction_date BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '14 days' -- add one day buffer
    GROUP BY 1,2,3
),

competitor_users AS (
    SELECT
        user_id,
        COUNT(DISTINCT competitor_name) AS competitor_count,
        LISTAGG(DISTINCT competitor_name, ' | ') within group (order by competitor_name) AS competitor_name_list,
        SUM(amount) AS competitor_total_amount
    FROM competitor_transactions
    GROUP BY 1
)

SELECT
    COALESCE(dave_advance.user_id, competitor_users.user_id) AS user_id,
    IFF(dave_advance.user_id IS NOT NULL, 1, 0) AS has_dave_request,
    dave_advance.is_approved_in_the_week,
    dave_advance.has_taken_in_the_week,
    dave_advance.max_approved_amount,
    IFF(competitor_users.user_id IS NOT NULL, 1, 0) AS if_borrowed_from_competitors,
    competitor_users.competitor_name_list,
    competitor_users.competitor_total_amount,
    competitor_users.competitor_count,
    IFF(competitor_users.competitor_total_amount > dave_advance.max_approved_amount, 1, 0) AS is_approved_higher_by_competitors,
    competitor_users.competitor_total_amount - dave_advance.max_approved_amount AS competitor_offer_than_dave_offer
    -- COUNT(*) AS request_count
FROM dave_advance
FULL OUTER JOIN competitor_users
    ON dave_advance.user_id = competitor_users.user_id
;


-- 1,155,348
SELECT
    COUNT(*)
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
;

SELECT
    has_dave_request,
    is_approved_in_the_week,
    has_taken_in_the_week,
    if_borrowed_from_competitors,
    COUNT(*)
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
GROUP BY 1,2,3,4
;


-- total borrowers	borrowed from Dave	borrowed from Dave only
-- 883,608	568,669	328,241
SELECT
    COUNT(*) AS "total borrowers",
    SUM(has_taken_in_the_week) AS "borrowed from Dave",
    SUM(IFF(has_taken_in_the_week = 1 AND if_borrowed_from_competitors = 0, 1, 0)) AS "borrowed from Dave only"
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
WHERE if_borrowed_from_competitors = 1
    OR has_taken_in_the_week = 1
;


--  315K users who didn't borrowed from Dave
SELECT
    competitor_count,
    has_dave_request,
    is_approved_in_the_week,
    UDF_OFFER_BUCKET(max_approved_amount) AS max_approved_amount_bucket,
    CASE
        WHEN competitor_offer_than_dave_offer IS NULL OR max_approved_amount = 0 THEN NULL
        WHEN competitor_offer_than_dave_offer < -200 THEN '0. < -200'
        WHEN competitor_offer_than_dave_offer < -100 THEN '1. [-200, -100)'
        WHEN competitor_offer_than_dave_offer < 0 THEN '2. [-100, 0)'
        WHEN competitor_offer_than_dave_offer = 0 THEN '3. equal'
        WHEN competitor_offer_than_dave_offer < 50 THEN '4. (0, 50)'
        WHEN competitor_offer_than_dave_offer < 100 THEN '5. [50, 100)'
        WHEN competitor_offer_than_dave_offer < 200 THEN '6. [100, 200)'
        ELSE '7. >=200' END AS competitor_offer_than_dave_offer,
    CASE
        WHEN competitor_offer_than_dave_offer IS NULL OR max_approved_amount = 0 THEN NULL
        WHEN competitor_offer_than_dave_offer < 0 THEN '-2. < 0'
        WHEN competitor_offer_than_dave_offer = 0 THEN '-1. equal'
        WHEN competitor_offer_than_dave_offer/max_approved_amount <= 0.25 THEN '1. <=25%'
        WHEN competitor_offer_than_dave_offer/max_approved_amount <= 0.5 THEN '2. <=50%'
        WHEN competitor_offer_than_dave_offer/max_approved_amount <= 0.75 THEN '3. <=75%'
        WHEN competitor_offer_than_dave_offer/max_approved_amount <= 1 THEN '4. <=100%'
        ELSE '5. >100%' END AS pctg_competitor_offer_than_dave_offer,
    is_approved_higher_by_competitors,
    COUNT(*)
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
WHERE if_borrowed_from_competitors = 1
    -- AND competitor_count = 1 -- only borrowed 1
    AND (has_taken_in_the_week = 0 OR has_taken_in_the_week IS NULL)
GROUP BY 1,2,3,4,5,6,7
;


--  281K users who didn't borrowed from Dave and also only borrowed 1 advance
-- competitor list
SELECT
    competitor_name_list,
    count(*) AS cnt,
    cnt / SUM(cnt) OVER () AS pctg
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
WHERE if_borrowed_from_competitors = 1
    AND competitor_count = 1 -- only borrowed 1
    AND (has_taken_in_the_week = 0 OR has_taken_in_the_week IS NULL)
GROUP BY 1
ORDER BY 2 DESC
;


-- slide 9
-- 23K users who didn't borrowed from Dave and also only borrowed 1 advance, requested and rejected by Dave
-- competitor list
-- COMPETITOR_NAME_LIST	CNT	PCTG
-- Chime	9,905	0.421095
-- Earnin	5,288	0.224811
-- Brigit	3,301	0.140337
-- Varo	1,595	0.067809
-- Albert	1,198	0.050931
-- Empower	1,175	0.049953
-- Money Lion	1,060	0.045064
SELECT
    competitor_name_list,
    count(*) AS cnt,
    cnt / SUM(cnt) OVER () AS pctg
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
WHERE if_borrowed_from_competitors = 1
    AND competitor_count = 1 -- only borrowed 1
    AND (has_taken_in_the_week = 0 OR has_taken_in_the_week IS NULL) -- didn't take Dave
    AND has_dave_request = 1 -- requsted Dave
    AND is_approved_in_the_week = 0 -- rejected by Dave
GROUP BY 1
ORDER BY 2 DESC
;

-- slide 9
-- 23K users who didn't borrowed from Dave and also only borrowed 1 advance, requested and rejected by Dave
-- COMPETITOR_NAME_LIST	CNT	PCTG
-- Chime	9,905	0.421095
WITH chime_rejected AS (
    SELECT
        *
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
    WHERE if_borrowed_from_competitors = 1
        AND competitor_count = 1 -- only borrowed 1
        AND (has_taken_in_the_week = 0 OR has_taken_in_the_week IS NULL) -- didn't take Dave
        AND has_dave_request = 1 -- requsted Dave
        AND is_approved_in_the_week = 0 -- rejected by Dave
        AND competitor_name_list = 'Chime' -- Chime users
),

dave_rejected_advance AS (
    SELECT
        user_id,
        primary_rejection_reason,
        rank() OVER (PARTITION BY user_id ORDER BY requested_ts DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_advance_approvals -- included EC
    WHERE requested_ds BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '13 days'
    QUALIFY rnk = 1
    -- approved and never taken in the week
    -- SELECT
    --     -- advance_requests.requested_ds,
    --     advance_requests.user_id,
    --     -- advance_requests.bank_account_id,
    --     -- MAX(IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL, 1, 0)) AS is_approved_in_the_week,
    --     -- MAX(COALESCE(advance_requests.max_approved_amount, 0)) AS max_approved_amount,
    --     -- MAX(IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0)) AS has_taken_in_the_week
    -- FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    -- -- LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
    -- --     ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    -- -- LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
    -- --     ON advance_requests.advance_approval_id = o2_takeout.approval_id
    -- WHERE advance_requests.requested_ds BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '13 days'
        -- AND TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL -- approved
    -- GROUP BY 1
    -- HAVING has_taken_in_the_week = 0 -- not takeout in the week
)

SELECT
    d.primary_rejection_reason,
    COUNT(DISTINCT c.user_id) AS cnt,
    cnt / SUM(cnt) OVER () AS pctg
FROM chime_rejected c
LEFT JOIN dave_rejected_advance d ON c.user_id = d.user_id
GROUP BY 1
ORDER BY 2 DESC
;


-- SELECT
--     user_id,
--     primary_rejection_reason
-- FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
-- WHERE advance_requests.requested_ds BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '13 days'
-- ;



-- 281K users who didn't borrowed from Dave and also only borrowed 1 advance
-- can we see their last approval amount and compare that to the amount they took from competitors to understand if we are losing on approval amount
-- 1	222,474 approved before

WITH not_request_one_advance AS (
    SELECT
        *
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
    WHERE if_borrowed_from_competitors = 1
        AND competitor_count = 1 -- only borrowed 1
        AND (has_taken_in_the_week = 0 OR has_taken_in_the_week IS NULL)
),

dave_last_approved_advance_request AS (
    SELECT
        advance_requests.requested_ds_pst,
        advance_requests.user_id,
        -- advance_requests.bank_account_id,
        -- IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL, 1, 0) AS is_approved,
        COALESCE(advance_requests.max_approved_amount, 0) AS approved_amount,
        IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0) AS is_taken,
        ROW_NUMBER() OVER (PARTITION BY advance_requests.user_id ORDER BY advance_requests.requested_ts DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
        ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
        ON advance_requests.advance_approval_id = o2_takeout.approval_id
    WHERE advance_requests.requested_ds_pst <= DATE($REQUEST_STARTING_DATE) + interval '13 days'
        AND TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL -- is approved
    QUALIFY rnk = 1 -- only last request
),

offer_compare AS (
    SELECT
        n.competitor_total_amount AS competitor_amount,
        d.approved_amount AS dave_amount,
        competitor_amount - dave_amount AS amount_diff,
        amount_diff/dave_amount AS amount_diff_pctg

        -- IFF(d.user_id IS NOT NULL, 1, 0) AS is_approved_before,
        -- COUNT(*)
    FROM not_request_one_advance n
    INNER JOIN dave_last_approved_advance_request d ON n.user_id = d.user_id
)

SELECT
    UDF_OFFER_BUCKET(dave_amount) AS dave_amount_bucket,
    IFF(amount_diff > 0, 1, 0) AS is_approved_higher_by_competitors,
    -- CASE
    --     WHEN amount_diff IS NULL OR max_approved_amount = 0 THEN NULL
    --     WHEN amount_diff < -200 THEN '0. < -200'
    --     WHEN amount_diff < -100 THEN '1. [-200, -100)'
    --     WHEN amount_diff < 0 THEN '2. [-100, 0)'
    --     WHEN amount_diff = 0 THEN '3. equal'
    --     WHEN amount_diff < 50 THEN '4. (0, 50)'
    --     WHEN amount_diff < 100 THEN '5. [50, 100)'
    --     WHEN amount_diff < 200 THEN '6. [100, 200)'
    --     ELSE '7. >=200' END AS competitor_offer_than_dave_offer,
    CASE
        WHEN amount_diff IS NULL THEN NULL
        WHEN amount_diff_pctg < 0 THEN '-2. < 0'
        WHEN amount_diff_pctg = 0 THEN '-1. equal'
        WHEN amount_diff_pctg <= 0.25 THEN '1. <=25%'
        WHEN amount_diff_pctg <= 0.5 THEN '2. <=50%'
        WHEN amount_diff_pctg <= 0.75 THEN '3. <=75%'
        WHEN amount_diff_pctg <= 1 THEN '4. <=100%'
        ELSE '5. >100%' END AS pctg_competitor_offer_than_dave_offer,
    COUNT(*)
FROM offer_compare
GROUP BY 1,2,3
;





--  884K total borrowers
SELECT
    if_borrowed_from_competitors,
    has_taken_in_the_week,
    COALESCE(competitor_count, 0) + COALESCE(has_taken_in_the_week, 0) AS total_offer,
    competitor_count,
    UDF_OFFER_BUCKET(max_approved_amount) AS dave_amount_bucket,
    UDF_OFFER_BUCKET(competitor_total_amount) AS competitor_total_amount_bucket,
    CASE
        WHEN competitor_offer_than_dave_offer IS NULL THEN NULL
        WHEN competitor_offer_than_dave_offer < -200 THEN '0. < -200'
        WHEN competitor_offer_than_dave_offer < -100 THEN '1. [-200, -100)'
        WHEN competitor_offer_than_dave_offer < 0 THEN '2. [-100, 0)'
        WHEN competitor_offer_than_dave_offer = 0 THEN '3. equal'
        WHEN competitor_offer_than_dave_offer < 50 THEN '4. (0, 50)'
        WHEN competitor_offer_than_dave_offer < 100 THEN '5. [50, 100)'
        WHEN competitor_offer_than_dave_offer < 200 THEN '6. [100, 200)'
        ELSE '7. >=200'
    END AS competitor_offer_than_dave_offer,
    is_approved_higher_by_competitors,
    COUNT(*)
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
WHERE if_borrowed_from_competitors = 1
    OR has_taken_in_the_week = 1
GROUP BY 1,2,3,4,5,6,7,8
;



-- WITH dave_advance AS (
--     -- approved and never taken in the week
--     SELECT
--         -- advance_requests.requested_ds,
--         advance_requests.user_id,
--         -- advance_requests.bank_account_id,
--         MAX(IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL, 1, 0)) AS is_approved_in_the_week,
--         MAX(COALESCE(advance_requests.max_approved_amount, 0)) AS max_approved_amount,
--         MAX(IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0)) AS has_taken_in_the_week
--     FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
--     LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
--         ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
--     LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
--         ON advance_requests.advance_approval_id = o2_takeout.approval_id
--     WHERE advance_requests.requested_ds BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '13 days'
--         -- AND TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL -- approved
--     GROUP BY 1
--     -- HAVING has_taken_in_the_week = 0 -- not takeout in the week
-- )

-- SELECT
--     UDF_OFFER_BUCKET(max_approved_amount) AS dave_amount_bucket,
--     COUNT(*) AS cnt,
--     cnt / SUM(cnt) OVER() AS pctg
-- FROM dave_advance
-- GROUP BY 1
-- ORDER BY 1
-- ;


-- 259K didn't request Dave but borrowed from competitors
-- past relation with Dave
WITH borrow_competitor_not_request_dave AS (
    SELECT
        *
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
    WHERE if_borrowed_from_competitors = 1
        AND has_dave_request = 0
),

dave_last_advance_request AS (
    SELECT
        advance_requests.requested_ds_pst,
        advance_requests.user_id,
        -- advance_requests.bank_account_id,
        IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL, 1, 0) AS is_approved,
        COALESCE(advance_requests.max_approved_amount, 0) AS approved_amount,
        IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0) AS is_taken,
        ROW_NUMBER() OVER (PARTITION BY advance_requests.user_id ORDER BY advance_requests.requested_ts DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
        ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
        ON advance_requests.advance_approval_id = o2_takeout.approval_id
    WHERE advance_requests.requested_ds_pst < $REQUEST_STARTING_DATE
    QUALIFY rnk = 1 -- only last request
),

dave_last_advance_takeout AS (
    SELECT
        user_id,
        disbursement_ds_pst,
        advance_amount,
        payback_ds_pst,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursement_ds_pst DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_advances
    WHERE disbursement_ds_pst < $REQUEST_STARTING_DATE
    QUALIFY rnk = 1 -- only last request

    UNION

    SELECT
        user_id,
        disbursement_ds_pst,
        overdraft_amount AS advance_amount,
        settlement_due_ds AS payback_ds_pst,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursement_ds_pst DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_overdraft_disbursement
    WHERE disbursement_ds_pst < $REQUEST_STARTING_DATE
    QUALIFY rnk = 1 -- only last request
),

advance_user_segment AS (
    SELECT
        user_id,
        user_label AS user_segment,
        has_outstanding_advance,
        first_advance_date,
        last_advance_date
    FROM dbt.adv_churn_marts.fct_adv_segment
    WHERE date_of_interest = $REQUEST_STARTING_DATE
)


SELECT
    IFF(s.user_id IS NOT NULL, 1, 0) AS is_borrowed_from_dave_before,
    s.has_outstanding_advance AS is_dave_outstanding,
    IFF(dlr.user_id IS NOT NULL, 1, 0) AS requested_before,
    IFF(dlt.payback_ds_pst < dlr.requested_ds_pst, 1, 0) AS is_requested_later,
    dlr.is_approved AS last_request_approved,
    s.user_segment,
    COUNT(*) AS cnt
FROM borrow_competitor_not_request_dave b
-- LEFT JOIN dave_takeouts dt ON b.user_id = dt.user_id
LEFT JOIN dave_last_advance_request dlr ON b.user_id = dlr.user_id
LEFT JOIN dave_last_advance_takeout dlt ON b.user_id = dlt.user_id
LEFT JOIN advance_user_segment s ON b.user_id = s.user_id
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6
;




-- 85K didn't request after last take out
-- are they monthly users? do they take out btw 4/15-30
WITH borrow_competitor_not_request_dave AS (
    SELECT
        *
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
    WHERE if_borrowed_from_competitors = 1
        AND has_dave_request = 0
),

dave_last_advance_request AS (
    SELECT
        advance_requests.requested_ds_pst,
        advance_requests.user_id,
        -- advance_requests.bank_account_id,
        IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL, 1, 0) AS is_approved,
        COALESCE(advance_requests.max_approved_amount, 0) AS approved_amount,
        IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0) AS is_taken,
        ROW_NUMBER() OVER (PARTITION BY advance_requests.user_id ORDER BY advance_requests.requested_ts DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
        ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
        ON advance_requests.advance_approval_id = o2_takeout.approval_id
    WHERE advance_requests.requested_ds_pst < $REQUEST_STARTING_DATE
    QUALIFY rnk = 1 -- only last request
),

dave_last_advance_takeout AS (
    SELECT
        user_id,
        disbursement_ds_pst,
        advance_amount,
        payback_ds_pst,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursement_ds_pst DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_advances
    WHERE disbursement_ds_pst < $REQUEST_STARTING_DATE
    QUALIFY rnk = 1 -- only last request

    UNION

    SELECT
        user_id,
        disbursement_ds_pst,
        overdraft_amount AS advance_amount,
        settlement_due_ds AS payback_ds_pst,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursement_ds_pst DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_overdraft_disbursement
    WHERE disbursement_ds_pst < $REQUEST_STARTING_DATE
    QUALIFY rnk = 1 -- only last request
),

advance_user_segment AS (
    SELECT
        user_id,
        user_label AS user_segment,
        has_outstanding_advance,
        first_advance_date,
        last_advance_date
    FROM dbt.adv_churn_marts.fct_adv_segment
    WHERE date_of_interest = $REQUEST_STARTING_DATE
),

credit_active_users_later AS (
    SELECT
        DISTINCT user_id
    FROM ANALYTIC_DB.DBT_metrics.credit_active_users
    WHERE transacting_ds_pst BETWEEN DATE($REQUEST_STARTING_DATE) + interval '14 days' AND DATE($REQUEST_STARTING_DATE) + interval '29 days'
)

SELECT
    IFF(cl.user_id IS NOT NULL, 1, 0) AS borrowed_later,
    COUNT(*)
FROM borrow_competitor_not_request_dave b
LEFT JOIN dave_last_advance_request dlr ON b.user_id = dlr.user_id
LEFT JOIN dave_last_advance_takeout dlt ON b.user_id = dlt.user_id
LEFT JOIN advance_user_segment s ON b.user_id = s.user_id
LEFT JOIN credit_active_users_later cl ON b.user_id = cl.user_id
WHERE
    s.user_id IS NOT NULL -- borrowed before
    AND s.has_outstanding_advance = 0 -- no outstanding'
    AND dlt.payback_ds_pst >= dlr.requested_ds_pst -- is_requested_later = 0
    -- AND s.user_segment = 'current user'
GROUP BY 1
;




-- out of 869K borrowers, how many from each
-- DAVE	ALBERT	BRIGIT	EMPOWER	EARNIN	CHIME	MONEY LION	VARO	REQUEST_CNT
-- 568,669	47,169	141,343	70,050	233,221	175,477	21,614	23,280	883,608
SELECT
    SUM(has_taken_in_the_week) AS dave,
    SUM(IFF(CONTAINS(competitor_name_list, 'Albert'), 1, 0)) AS Albert,
    SUM(IFF(CONTAINS(competitor_name_list, 'Brigit'), 1, 0)) AS Brigit,
    SUM(IFF(CONTAINS(competitor_name_list, 'Empower'), 1, 0)) AS Empower,
    SUM(IFF(CONTAINS(competitor_name_list, 'Earnin'), 1, 0)) AS Earnin,
    SUM(IFF(CONTAINS(competitor_name_list, 'Chime'), 1, 0)) AS Chime,
    SUM(IFF(CONTAINS(competitor_name_list, 'Money Lion'), 1, 0)) AS "MONEY LION",
    SUM(IFF(CONTAINS(competitor_name_list, 'Varo'), 1, 0)) AS Varo,
    COUNT(*) AS request_cnt
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
WHERE if_borrowed_from_competitors = 1
    OR has_taken_in_the_week = 1
;



-- out of 884K borrowers,

SELECT
    COALESCE(competitor_count, 0) AS competitor_offer_count,
    COALESCE(has_taken_in_the_week, 0) AS has_taken_dave,
    COALESCE(competitor_count, 0) + COALESCE(has_taken_in_the_week, 0) AS total_offer_cnt,
    COUNT(*) AS cnt
FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
WHERE if_borrowed_from_competitors = 1
    OR has_taken_in_the_week = 1
GROUP BY 1,2,3
;


-- 15K taking competitor not Dave, & were offered lower amount, how many of them have multiple income?
WITH lower_offer AS (
    SELECT
        user_id
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w -- advance_approved_not_taken_20220401_2w
    WHERE
        has_dave_request = 1
        AND is_approved_in_the_week = 1
        AND has_taken_in_the_week = 0
        AND competitor_count = 1 -- take one of 6 competitors
        AND is_approved_higher_by_competitors = 1
),

income AS (
    SELECT
          user_id,
          COUNT(*) AS income_cnt
      FROM APPLICATION_DB.TRANSACTIONS_DAVE.RECURRING_TRANSACTION
      WHERE type = 'INCOME'
        AND STATUS = 'VALID'
        -- AND user_id = 4816563
        AND to_date(deleted) > $REQUEST_STARTING_DATE
    GROUP BY 1
)

SELECT
    COALESCE(income.income_cnt, 0) AS income_cnt,
    COUNT(*)
FROM lower_offer
LEFT JOIN income ON lower_offer.user_id = income.user_id
GROUP BY 1
ORDER BY 1
;




-- for 38K didn't request Dave, check their signup date (hypothesis: they were acquired in last summer's banking campaigns)
WITH borrow_competitor_not_request_dave AS (
    SELECT
        *
    FROM DBT.DEV_HU_PUBLIC.advance_competitor_20220401_2w
    WHERE if_borrowed_from_competitors = 1
        AND has_dave_request = 0
),

dave_last_advance_request AS (
    SELECT
        advance_requests.requested_ds_pst,
        advance_requests.user_id,
        -- advance_requests.bank_account_id,
        IFF(TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL, 1, 0) AS is_approved,
        COALESCE(advance_requests.max_approved_amount, 0) AS approved_amount,
        IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0) AS is_taken,
        ROW_NUMBER() OVER (PARTITION BY advance_requests.user_id ORDER BY advance_requests.requested_ts DESC) AS rnk
    FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
    LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
        ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
    LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
        ON advance_requests.advance_approval_id = o2_takeout.approval_id
    WHERE advance_requests.requested_ds_pst < $REQUEST_STARTING_DATE
    QUALIFY rnk = 1 -- only last request
),

never_request AS (
    SELECT
        b.user_id
    FROM borrow_competitor_not_request_dave b
    LEFT JOIN dave_last_advance_request dlr ON b.user_id = dlr.user_id
    WHERE dlr.user_id IS NULL
),

user_info AS (
    SELECT
        n.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', u.created::timestamp_ntz)) AS created_pt_dt,
        IFF(created_pt_dt > DATE($REQUEST_STARTING_DATE) + interval '13 days', 1, 0) AS created_after_analysis,
        TO_VARCHAR(created_pt_dt, 'yyyy-mm') AS created_month
    FROM never_request n
    LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER u ON n.user_id = u.id
)

SELECT
    IFF(created_after_analysis = 1, 'created_after
        ', created_month) AS created_category,
    COUNT(*) AS cnt
FROM user_info
GROUP BY 1
ORDER BY 1
;

-- SELECT
--     id AS user_id,
--     CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS created_pt_ts,
--     DATE(created_pt_ts) AS created_pt_ds,
--     state,
--     city,
--     gender,
--     email_verified AS is_email_verified,
--     birthdate,
--     FLOOR(DATEDIFF(DAY, birthdate, current_date) / 365.0) AS age,
--     *
-- FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER
-- WHERE created_pt_ds BETWEEN $ANALYSIS_STARTING_DATE AND $ANALYSIS_END_DATE
--     AND _fivetran_deleted = 'FALSE'
-- LIMIT 10
-- ;


CREATE OR REPLACE FUNCTION UDF_CLOSEST_PERIOD(active_date VARCHAR, end_date VARCHAR)
    RETURNS DATE AS
$$
    DATEADD('d', -14 * FLOOR(DATEDIFF(DAY, DATE(active_date), DATE(end_date)) / 14) - 13, DATE(end_date))
$$;


-- SELECT
--     -- FLOOR(DATEDIFF(DAY, DATE('2022-04-13'), DATE('2022-04-14')) / 14) ,
--     DATEADD('d', -14 * FLOOR(DATEDIFF(DAY, DATE('2022-03-23'), DATE('2022-04-14')) / 14) - 13, DATE('2022-04-14'))
-- ;

SET END_ANALYSIS_DATE = '2022-04-14';


--- biweekly borrowers
CREATE OR REPLACE TABLE DBT.DEV_HU_PUBLIC.advance_biweekly_borrowers AS
WITH dave_takens AS (
    SELECT
        DISTINCT
             UDF_CLOSEST_PERIOD(transacting_ds_pst, $END_ANALYSIS_DATE) AS period_date,
             user_id,
             'dave' AS company_name
    FROM ANALYTIC_DB.DBT_metrics.credit_active_users
    WHERE transacting_ds_pst BETWEEN '2021-01-01' AND $END_ANALYSIS_DATE
),

competitor_taken AS (
    SELECT
        DISTINCT
            UDF_CLOSEST_PERIOD(transaction_date, $END_ANALYSIS_DATE) AS period_date,
            user_id,
            UDF_COMPETITOR(display_name, amount) AS company_name
    FROM datastream_prd.dave.bank_transaction
    WHERE company_name IS NOT NULL -- borrow from competitors
        AND amount >= 1 -- positive and at least 1 dollar
        AND transaction_date BETWEEN '2021-01-01' AND $END_ANALYSIS_DATE
    GROUP BY 1,2,3
),

dave_competitor_taken AS (
    SELECT
        *
    FROM dave_takens

    UNION

    SELECT
        *
    FROM competitor_taken
)


SELECT
    period_date,
    user_id,
    COUNT(DISTINCT company_name) AS competitor_count,
    LISTAGG(DISTINCT company_name, ' | ') within group (order by company_name) AS company_name_list
FROM dave_competitor_taken
GROUP BY 1,2
;


SELECT
    *
FROM DBT.DEV_HU_PUBLIC.advance_biweekly_borrowers
LIMIT 10
;


SELECT
    period_date,
    SUM(IFF(CONTAINS(company_name_list, 'Albert'), 1, 0)) AS Albert,
    SUM(IFF(CONTAINS(company_name_list, 'Brigit'), 1, 0)) AS Brigit,
    SUM(IFF(CONTAINS(company_name_list, 'Empower'), 1, 0)) AS Empower,
    SUM(IFF(CONTAINS(company_name_list, 'Earnin'), 1, 0)) AS Earnin,
    SUM(IFF(CONTAINS(company_name_list, 'Chime'), 1, 0)) AS Chime,
    SUM(IFF(CONTAINS(company_name_list, 'Money Lion'), 1, 0)) AS "MONEY LION",
    SUM(IFF(CONTAINS(company_name_list, 'Varo'), 1, 0)) AS Varo,
    SUM(IFF(CONTAINS(company_name_list, 'dave'), 1, 0)) AS Dave,
    COUNT(*) AS total_borrower
FROM DBT.DEV_HU_PUBLIC.advance_biweekly_borrowers
GROUP BY 1
ORDER BY 1
;

-- SELECT
--     *
-- FROM DBT.DEV_HU_PUBLIC.advance_approved_not_taken_20220401
-- LIMIT 10
-- ;


-- -- USER_ID	HAS_TAKEN_IN_THE_WEEK	MAX_APPROVED_AMOUNT	IF_BORROWED_FROM_COMPETITORS	COMPETITOR_NAME_LIST	COMPETITOR_TOTAL_AMOUNT	COMPETITOR_COUNT	IS_APPROVED_HIGHER_BY_COMPETITORS	COMPETITOR_OFFER_THAN_DAVE_OFFER
-- -- 4816563	0	10	1	Chime	1,480	1	1	1,470
-- -- 10811612	0	20	1	Chime	560	1	1	540
-- -- 7254167	0	5	1	Chime	720	1	1	715
-- -- 7381206	0	25	1	Chime	1,160	1	1	1,135
-- -- 11352168	0	10	1	Chime	1,225	1	1	1,215
-- -- 2947342	0	10	1	Chime	525	1	1	515
-- -- 2563028	0	25	1	Chime	555	1	1	530
-- -- 12047270	0	5	1	Chime	685	1	1	680
-- -- 10780262	0	10	1	Chime	555	1	1	545
-- -- 3599430	0	5	1	Chime	1,405	1	1	1,400
-- SELECT
--     *
-- FROM DBT.DEV_HU_PUBLIC.advance_approved_not_taken_20220401
-- WHERE
--     -- (has_taken_in_the_week = 1 AND if_borrowed_from_competitors = 0) -- take Dave offer only
--     -- OR
--     (has_taken_in_the_week = 0 AND competitor_count = 1) -- take one of 6 competitors
--     AND  max_approved_amount <= 25 -- '1. <= $25'
--     AND is_approved_higher_by_competitors = 1
--     AND competitor_offer_than_dave_offer > 500
-- LIMIT 10
-- ;

-- SET INVERSTIGATE_USER_ID = ;

-- SELECT
--     transaction_date,
--     user_id,
--     -- bank_account_id,
--     display_name,
--     -- UDF_COMPETITOR(display_name, amount) AS competitor_name,
--     amount AS amount,
--     *
-- FROM datastream_prd.dave.bank_transaction
-- WHERE amount >= 1 -- positive and at least 1 dollar
--     AND transaction_date BETWEEN DATE($REQUEST_STARTING_DATE) - interval '7 days' AND DATE($REQUEST_STARTING_DATE) + interval '7 days' -- add one day buffer
--     AND user_id = 4816563
--     ORDER BY 1
--     ;


-- -- ID	BANK_ACCOUNT_ID	USER_AMOUNT	ANNUAL_INCOME	USER_AMOUNT_RNK	ID_2	INTERVAL	USER_AMOUNT_2	TERMINATED	DELETED	CREATED	USER_DISPLAY_NAME	UPDATED	ROLL_DIRECTION	BANK_ACCOUNT_ID_2	USER_ID	STATUS	MISSED	PARAMS	POSSIBLE_NAME_CHANGE	PENDING_DISPLAY_NAME	DTSTART	TYPE	TRANSACTION_DISPLAY_NAME	_FIVETRAN_DELETED	SKIP_VALIDITY_CHECK	_FIVETRAN_SYNCED
-- -- 71710779	1645968927	449	23,348	1	71710779	WEEKLY	449		9999-12-31 23:59:59.000	2022-01-19 15:57:03.000	Circle K Payroll	2022-03-18 09:53:35.000	-1	1645968927	4816563	VALID	2022-03-18 09:53:35.000	[   "wednesday" ]			2021-12-22	INCOME	Circle K Payroll	FALSE	FALSE	2022-03-18 14:28:19.465 +0000
-- SELECT
--       id,
--       bank_account_id,
--       user_amount,
--       CASE WHEN interval IN ('MONTHLY', 'WEEKDAY_MONTHLY') THEN user_amount * 12
--         WHEN interval = 'SEMI_MONTHLY' THEN user_amount * 24
--         WHEN interval = 'BIWEEKLY' THEN user_amount *  26
--         WHEN interval = 'WEEKLY' THEN user_amount * 52
--       ELSE 0 END AS annual_income,
--       rank() OVER (PARTITION BY bank_account_id ORDER BY annual_income DESC) AS user_amount_rnk,
--       *
--   FROM APPLICATION_DB.TRANSACTIONS_DAVE.RECURRING_TRANSACTION
--   WHERE type = 'INCOME'
--     -- AND STATUS = 'VALID'
--     AND user_id = 4816563
--     AND to_date(deleted) > $REQUEST_STARTING_DATE
-- LIMIT 100
-- ;

-- SELECT
--     advance_requests.*
--         -- advance_requests.requested_ds,
--         -- advance_requests.user_id,
--         -- advance_requests.bank_account_id,
--         -- MAX(advance_requests.max_approved_amount) AS max_approved_amount,
--         -- MAX(IFF(advance_takeout.chosen_advance_approval_id IS NOT NULL OR o2_takeout.approval_id IS NOT NULL, 1, 0)) AS has_taken_in_the_week
--     FROM analytic_db.dbt_marts.fct_advance_approvals AS advance_requests -- included EC
--     -- LEFT JOIN analytic_db.dbt_marts.fct_advances AS advance_takeout -- legacy advance only
--     --     ON advance_requests.advance_approval_id = advance_takeout.chosen_advance_approval_id
--     -- LEFT JOIN analytic_db.dbt_marts.fct_overdraft_disbursement AS o2_takeout -- EC overdraft only
--     --     ON advance_requests.advance_approval_id = o2_takeout.approval_id
--     WHERE advance_requests.requested_ds BETWEEN $REQUEST_STARTING_DATE AND DATE($REQUEST_STARTING_DATE) + interval '6 days'
--         -- AND TO_NUMERIC(advance_requests.max_approved_amount) IS NOT NULL -- approved
--         AND user_id = 4816563
-- ;

-- SELECT *
-- FROM APPLICATION_DB.TRANSACTIONS_DAVE.EXPECTED_TRANSACTION
-- WHERE USER_ID = 4816563
--     AND RECURRING_TRANSACTION_ID = 71710779
-- ORDER BY expected_date
-- LIMIT 1000
-- ;

-- WITH advances AS (
--     SELECT
--          user_id,
--          COUNT(*) AS advance_cnt
--     FROM ANALYTIC_DB.DBT_metrics.credit_active_users
--     GROUP BY 1
-- )

-- SELECT
--     advance_cnt,
--     COUNT(*) AS user_cnt,
--     user_cnt / SUM(user_cnt) OVER() AS user_pctg
-- FROM advances
-- GROUP BY 1
-- ORDER BY 1
-- ;

SELECT *
FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_TRANSACTION
LIMIT 10
;