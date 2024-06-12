create or replace table sandbox.dev_yyang.lossrate_disbursements as
WITH

    fct_advance_approvals AS (

        SELECT * FROM ANALYTIC_DB.DBT_marts.fct_advance_approvals_dedup

    ),

    fct_advances AS (

        SELECT * FROM ANALYTIC_DB.DBT_marts.fct_advances

    ),

    fct_overdraft_disbursement AS (

        SELECT * FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement

    ),

    disbursements__unioned AS (
        SELECT DISTINCT
            advance_id::VARCHAR                                     AS advance_id,
            chosen_advance_approval_id                              AS advance_approval_id,
            user_id,
            disbursement_ds,
            disbursement_ts_pst,
            advance_amount                                          AS taken_amount

        FROM fct_advances

        UNION ALL

        SELECT DISTINCT
            overdraft_id::VARCHAR        AS advance_id,
            approval_id                  AS advance_approval_id,
            user_id,
            disbursement_ds,
            disbursement_ts_pst,
            overdraft_amount             AS taken_amount
        FROM fct_overdraft_disbursement

    ),

    final AS (
        SELECT DISTINCT
            advance_id,
            advance_approval_id,
            user_id,
            disbursement_ds,
            taken_amount,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY disbursement_ts_pst ASC,
                         advance_approval_id ASC
            )                                                       AS ith_advance_taken

        FROM disbursements__unioned
    )

SELECT * FROM final