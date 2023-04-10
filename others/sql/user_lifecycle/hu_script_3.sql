-- original UDF by Song
-- create function UDF_IS_COMPETITOR(DESCRIPTION VARCHAR)
--     returns BOOLEAN
-- as
-- $$
--     -- Top competitior: Albert, Brigit, Empower, Earnin, Chime --
--     CASE WHEN LOWER(description) LIKE 'albert instant%' OR LOWER(description) LIKE 'albert savings%' THEN true
--          WHEN LOWER(description) LIKE '%brigit%' THEN true
--          WHEN LOWER(description) LIKE '%empower%' THEN true
--          WHEN LOWER(description) LIKE '%earnin%' AND LOWER(description) NOT LIKE '%learnin%' THEN true
--          WHEN LOWER(description) LIKE '%chime%' THEN true
--          WHEN LOWER(description) LIKE '%money%lion%' THEN true
--          ELSE false
--     END
-- $$;

-- CREATE OR REPLACE FUNCTION UDF_IS_COMPETITOR(DESCRIPTION VARCHAR)
--     returns BOOLEAN
-- as
-- $$
--     -- Top competitior: Albert, Brigit, Empower, Earnin, Chime --
--     CASE WHEN LOWER(description) LIKE 'albert instant%' OR LOWER(description) LIKE 'albert savings%' THEN true
--          WHEN LOWER(description) LIKE '%brigit%' THEN true
--          WHEN LOWER(description) LIKE '%empower%' THEN true
--          WHEN LOWER(description) LIKE '%earnin%' AND LOWER(description) NOT LIKE '%learnin%' THEN true
--          WHEN LOWER(description) LIKE '%chime%'
--             AND LOWER(description) NOT LIKE '%transfer from chime savings account%'
--             AND LOWER(description) NOT LIKE '%transfer from chime checking account%'
--             THEN true
--          WHEN LOWER(description) LIKE '%money%lion%' THEN true
--          ELSE false
--     END
-- $$;

/*
    function to infer whether the transaction is to take Advance from known competitors
    return a competitor name if yes, otherwise NULL
*/
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


-- COMPETITOR_NAME	AMOUNT	USER_CNT	USER_PCTG
-- 	77,795,139.2	1,705,611	0.915497
-- Chime	9,041,785	52,250	0.028046
-- Earnin	7,180,225	71,882	0.038583
-- Brigit	1,067,250	15,264	0.008193
-- Empower	945,475	8,261	0.004434
-- Money Lion	826,975	4,217	0.002264
-- Albert	470,155	5,558	0.002983
SELECT
    UDF_COMPETITOR(display_name, amount) AS competitor_name,
    SUM(amount) AS amount,
    COUNT(DISTINCT user_id) AS user_cnt,
    user_cnt / SUM(user_cnt) OVER () AS user_pctg
FROM datastream_prd.dave.bank_transaction
WHERE transaction_date = '2022-04-01'
GROUP BY 1
ORDER BY 2 DESC
;


SELECT
    UDF_COMPETITOR(display_name, amount) AS competitor_name,
    display_name,
    amount,
    *
FROM datastream_prd.dave.bank_transaction
WHERE transaction_date = '2022-04-01'
    AND competitor_name IS NOT NULL
    AND competitor_name = 'Chime'
LIMIT 100
;

SELECT
    UDF_COMPETITOR(display_name, amount) AS competitor_name,
    display_name,
    COUNT(*)
FROM datastream_prd.dave.bank_transaction
WHERE transaction_date = '2022-04-01'
    AND competitor_name IS NOT NULL
    AND competitor_name = 'Chime'
GROUP BY 1,2
;



SELECT
    UDF_COMPETITOR_REPAY(display_name, amount) AS repay_competitor_name,
    display_name,
    amount,
    *
FROM datastream_prd.dave.bank_transaction
WHERE transaction_date = '2022-04-01'
    -- AND competitor_name IS NOT NULL
    -- AND competitor_name = 'Chime'
LIMIT 100
;



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