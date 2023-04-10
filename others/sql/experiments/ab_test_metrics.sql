-- cohorts are defined here https://app.snowflake.com/us-west-2/qc63563/w2qYCftKhDrh#query

WITH
-- total relaunch population, expect for brand new users
-- Cohorts to test:
-- - Cohort 1 [EXISTING] non-converted PV users
-- - Cohort 2 [EXISTING] deep dormants
-- - Cohort 3 [EXISTING] opted into CBv1
-- - Cohort 4 [EXISTING] active users, not opted into CBv1
relaunch_cohorts AS (
    SELECT user_id, 'cohort 1: non-converted PV' as cohort_name
    FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort1

    UNION

    SELECT user_id, 'cohort 2: deep dormant' as cohort_name
    FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort2

    UNION

    SELECT user_id, 'cohort 3: opted into CBv1' as cohort_name
    FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort3

    UNION

    SELECT user_id, 'cohort 4: active users, not opted into CBv1' as cohort_name
    FROM DBT.DEV_HU_PUBLIC.cb_relaunch_cohort4
),

-- a/b assignment
ab_assign AS (
    SELECT
        user_id,
        event_properties:"extra-cash-credit-builder.variant" AS test_variant,
        MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time)) AS start_ts_pt
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    WHERE EVENT_TYPE = '[Experiment] Assignment'
        AND event_time >= '2023-03-20'
        AND event_properties:"extra-cash-credit-builder.variant" in ('control', 'enabled')
    GROUP BY 1,2
),

-- advance taken
advance_taken AS (
    SELECT
        user_id,
        disbursement_ts_pst
    FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement
    WHERE disbursement_ds_pst >= '2023-03-20' -- rollout date
        AND disbursement_status = 'COMPLETE'
),

outstanding_advance AS (
    SELECT
        created_date,
        user_id
    FROM ANALYTIC_DB.DBT_marts.fct_ExtraCash_Collection
    WHERE outstanding_status = 'yes'
        AND created_date >= '2023-03-20' -- rollout date
),

user_data AS (
    SELECT
        ab_assign.user_id AS user_id,
        COALESCE(relaunch_cohorts.cohort_name, 'cohort 5: new PV') AS cohort,
        DATE(ab_assign.start_ts_pt) AS ab_test_dt_pst,
        ab_assign.test_variant AS test_variant,
        MAX(IFF(DATE(advance_taken.disbursement_ts_pst) <= DATE(ab_assign.start_ts_pt) + 29, 1, 0)) AS is_30d_taken,  -- 30 days advance taken
        MAX(IFF(outstanding_advance.user_id IS NOT NULL, 1, 0)) AS is_payoff,
        COUNT(DISTINCT advance_taken.disbursement_ts_pst) AS advance_taken_cnt
    FROM ab_assign
    LEFT JOIN relaunch_cohorts ON ab_assign.user_id = relaunch_cohorts.user_id
    LEFT JOIN advance_taken
        ON ab_assign.user_id = advance_taken.user_id
            AND ab_assign.start_ts_pt <= advance_taken.disbursement_ts_pst --the experiment assignment before disbursement
            -- AND DATE(advance_taken.disbursement_ts_pst) <= DATE(ab_assign.start_ts_pt) + 29 -- within 30 days
    LEFT JOIN outstanding_advance ON ab_assign.user_id = outstanding_advance.user_id
            AND ab_assign.start_ts_pt <= outstanding_advance.created_date --the experiment assignment before disbursement
    GROUP BY 1,2,3,4
)

SELECT
    cohort,
    ab_test_dt_pst,
    test_variant,
    COUNT(*) AS user_cnt,
    SUM(is_30d_taken) AS advance_taken_user_cnt,
    SUM(IFF(is_30d_taken > 0 AND is_payoff > 0, 1, 0)) AS payoff_user_cnt,
    SUM(IFF(advance_taken_cnt > 1, 1, 0)) AS sec_advance_user_cnt
FROM user_data
GROUP BY 1,2,3
ORDER BY 1,2,3
;