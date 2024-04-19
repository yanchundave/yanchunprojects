WITH cohort_items AS (
    SELECT
        DATE_TRUNC('MONTH', transacting_ds_pst) AS cohort_month,
        user_id
    FROM ANALYTIC_DB.DBT_metrics.new_transacting_users
    WHERE transacting_ds_pst < DATE_TRUNC('MONTH', CURRENT_DATE())
),

settlement AS ( -- this is for the outstanding amount past due date
    SELECT
        advance_id,
        IFF(outstanding_amount > 0, outstanding_amount, 0) AS outstanding_amount -- to take care of over settlement credit txns
    FROM ANALYTIC_DB.DBT_marts.settlements
    WHERE settlement_due_ds_pst < CURRENT_DATE()
),

settlement_30D_past_due AS ( -- this is for the outstanding amount past 30 days after due date
    SELECT
        advance_id,
        IFF(outstanding_amount > 0, outstanding_amount, 0) AS outstanding_amount -- to take care of over settlement credit txns
    FROM ANALYTIC_DB.DBT_marts.settlements
    WHERE settlement_due_ds_pst < DATEADD(DAY, -30, CURRENT_DATE())
),

credit_record AS (
    SELECT
        dis.product,
        dis.user_id,
        dis.advance_id,
        DATE_TRUNC('MONTH', dis.disbursement_ds_pst) AS disburse_month,
        cohort.cohort_month,
        MONTHS_BETWEEN(disburse_month, cohort.cohort_month) AS month_number,
        dis.taken_amount AS advance_amount,
        dis.tip_amount AS tip,
        dis.express_fee_amount + dis.service_fee_amount AS fee,
        sett.outstanding_amount,
        sett_30D.outstanding_amount AS outstanding_amount_30D
    FROM ANALYTIC_DB.DBT_marts.disbursements dis
        LEFT JOIN cohort_items cohort
            ON dis.user_id = cohort.user_id  AND dis.disbursement_ds_pst >= cohort.cohort_month
        LEFT JOIN settlement sett
            ON dis.advance_id = sett.advance_id
        LEFT JOIN settlement_30D_past_due sett_30D
            ON dis.advance_id = sett_30D.advance_id
    WHERE dis.disbursement_ds_pst < DATE_TRUNC('MONTH', CURRENT_DATE())
)

SELECT
    credit.product AS product_type,
    credit.cohort_month AS first_txn_month,
    credit.month_number,
    COUNT(DISTINCT credit.user_id) AS unique_borrowers,
    COUNT(DISTINCT credit.advance_id) AS num_advances_disbursed,
    SUM(credit.advance_amount) AS principal_disbursed,
    SUM(credit.tip) AS pledged_tip,
    SUM(credit.fee) AS pledged_fee,
    SUM(credit.outstanding_amount) AS uncollected_receivables,
    SUM(credit.outstanding_amount_30D) AS uncollected_receivables_30D
FROM credit_record credit
GROUP BY 1, 2, 3