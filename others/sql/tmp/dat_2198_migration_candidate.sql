with monthly_sub AS (

    SELECT * FROM ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
),

user_kyc_check AS (

    SELECT *  FROM dave.identity.user_kyc_check
),

user_kyc_decision_type AS (

    SELECT * FROM dave.identity.user_kyc_decision_type
),
---kyc decison
kyc_decision AS (

  SELECT
    convert_timezone('UTC','America/Los_Angeles', u.created::timestamp_ntz) as kyc_created_ts_pst,
    NAME,
    DAVE_USER_ID AS user_id,
    row_number () over (partition by DAVE_USER_ID order by convert_timezone('UTC','America/Los_Angeles', u.created::timestamp_ntz) desc) rn
  FROM user_kyc_check u
  INNER JOIN user_kyc_decision_type ut ON u.DECISION_TYPE_ID = ut. ID
  QUALIFY rn = 1 -- get last KYC decision

),

april_subs AS (

    SELECT
        DISTINCT USER_ID
    FROM monthly_sub
    WHERE BILLING_CYCLE = '2023-04'
        AND IS_COLLECT_SUCCEEDED = 1
        AND PAYMENT_METHOD = 'ACH'
),

may_subs AS (

    SELECT
        DISTINCT USER_ID
    FROM monthly_sub
    WHERE BILLING_CYCLE = '2023-05'
        AND IS_COLLECT_SUCCEEDED = 1
        AND PAYMENT_METHOD = 'ACH'
),

outstanding AS (

    SELECT
        april_subs.USER_ID
    FROM april_subs
    JOIN may_subs
        ON april_subs.USER_ID = may_subs.USER_ID
),

jun_subs AS (

    SELECT
        USER_ID,
        BC_HEALTHINESS_SCORE,
        BILL_DUE_DATE
    FROM monthly_sub
    WHERE BILLING_CYCLE = '2023-06'
        AND BC_HEALTHINESS_SCORE = 1
        AND BILL_DUE_DATE >= DATE('2023-06-20')
        AND IS_COLLECT_SUCCEEDED = 0
)

SELECT
    jun_subs.USER_ID,
    jun_subs.BC_HEALTHINESS_SCORE,
    jun_subs.BILL_DUE_DATE,
    'N' AS outstanding,
    kyc_decision.NAME AS kyc_name
FROM jun_subs
JOIN outstanding
    ON jun_subs.USER_ID = outstanding.USER_ID
JOIN kyc_decision
    ON jun_subs.USER_ID = kyc_decision.USER_ID
WHERE kyc_decision.NAME = 'accept'