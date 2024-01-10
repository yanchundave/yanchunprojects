WITH

one_dave_transacting_users AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_transacting_users

),

dim_dates AS (

    SELECT * FROM ANALYTIC_DB.DBT_marts.dim_dates

),

cnt_bank_credit_txn_monthly AS (
    SELECT
        DATE_TRUNC(MONTH, transacting_ds_pst) AS reporting_month,
        user_id,
        SUM(IFF(type_code = 'Bank', 1, 0)) AS bank_txn_cnt,
        SUM(IFF(type_code = 'Credit(Advance & Extra Cash)', 1, 0)) AS credit_txn_cnt
    FROM one_dave_transacting_users
    GROUP BY 1, 2
),

cnt_bank_credit_txn_weekly AS (
    SELECT
        DATE_TRUNC(WEEK, transacting_ds_pst) AS reporting_week,
        user_id,
        SUM(IFF(type_code = 'Bank', 1, 0)) AS bank_txn_cnt,
        SUM(IFF(type_code = 'Credit(Advance & Extra Cash)', 1, 0)) AS credit_txn_cnt
    FROM one_dave_transacting_users
    GROUP BY 1, 2
),

credit_monthly AS (
    SELECT
        reporting_month,
        COUNT(DISTINCT user_id) AS credit_only_users_cnt
    FROM cnt_bank_credit_txn_monthly
    WHERE credit_txn_cnt > 0 AND (bank_txn_cnt IS NULL OR bank_txn_cnt = 0)
    GROUP BY 1
),

bank_monthly AS (
    SELECT
        reporting_month,
        COUNT(DISTINCT user_id) AS bank_only_users_cnt
    FROM cnt_bank_credit_txn_monthly
    WHERE (credit_txn_cnt IS NULL OR credit_txn_cnt = 0) AND bank_txn_cnt > 0
    GROUP BY 1
),

both_monthly AS (
    SELECT
        reporting_month,
        COUNT(DISTINCT user_id) AS both_users_cnt
    FROM cnt_bank_credit_txn_monthly
    WHERE credit_txn_cnt > 0 AND bank_txn_cnt > 0
    GROUP BY 1
),

final_monthly AS (
    SELECT DISTINCT
        dd.month_start_ds AS reporting_date,
        'Monthly' AS agglevel,
        COALESCE(cm.credit_only_users_cnt,0) AS credit_only_users_cnt,
        COALESCE(bm.bank_only_users_cnt,0) AS banking_only_users_cnt,
        COALESCE(bmm.both_users_cnt,0) AS both_users_cnt
    FROM dim_dates dd
    LEFT JOIN credit_monthly cm
      ON dd.month_start_ds = cm.reporting_month
    LEFT JOIN bank_monthly bm
      ON dd.month_start_ds = bm.reporting_month
    LEFT JOIN both_monthly bmm
      ON dd.month_start_ds = bmm.reporting_month
),

credit_weekly AS (
    SELECT
        reporting_week,
        COUNT(DISTINCT user_id) AS credit_only_users_cnt
    FROM cnt_bank_credit_txn_weekly
    WHERE credit_txn_cnt > 0 AND (bank_txn_cnt IS NULL OR bank_txn_cnt = 0)
    GROUP BY 1
),

bank_weekly AS (
    SELECT
        reporting_week,
        COUNT(DISTINCT user_id) AS bank_only_users_cnt
    FROM cnt_bank_credit_txn_weekly
    WHERE (credit_txn_cnt IS NULL OR credit_txn_cnt = 0) AND bank_txn_cnt > 0
    GROUP BY 1
),

both_weekly AS (
    SELECT
        reporting_week,
        COUNT(DISTINCT user_id) AS both_users_cnt
    FROM cnt_bank_credit_txn_weekly
    WHERE credit_txn_cnt > 0 AND bank_txn_cnt > 0
    GROUP BY 1
),

final_weekly AS (
    SELECT DISTINCT
        dd.week_start_ds AS reporting_date,
        'Weekly' AS agglevel,
        COALESCE(cw.credit_only_users_cnt,0) AS credit_only_users_cnt,
        COALESCE(bw.bank_only_users_cnt,0) AS banking_only_users_cnt,
        COALESCE(bww.both_users_cnt,0) AS both_users_cnt
    FROM dim_dates dd
    LEFT JOIN credit_weekly cw
      ON dd.week_start_ds = cw.reporting_week
    LEFT JOIN bank_weekly bw
      ON dd.week_start_ds = bw.reporting_week
    LEFT JOIN both_weekly bww
      ON dd.week_start_ds = bww.reporting_week
),

final AS (
    SELECT * FROM final_monthly

    UNION ALL

    SELECT * FROM final_weekly
)

SELECT * FROM final