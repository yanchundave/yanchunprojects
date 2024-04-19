WITH advance_25 AS (
    SELECT
        adv.advance_id AS credit_id,
        adv.disbursement_ds_pst,
        adv.advance_amount AS total_principal,
        adv.fee + adv.tip + adv.advance_amount AS total_receivable,
        SUM(pay.amount) AS total_collections_by_25
    FROM ANALYTIC_DB.DBT_marts.fct_advances adv
        LEFT JOIN APPLICATION_DB.TRANSACTIONS_DAVE.PAYMENT pay
            ON (adv.advance_id = pay.advance_id
                AND pay.status IN ('COMPLETED')
                AND TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', pay.created::timestamp_ntz)) <=
                    DATEADD(MONTH, 1, DATEFROMPARTS(YEAR(adv.disbursement_ds_pst), MONTH(adv.disbursement_ds_pst), 25))
                AND pay._FIVETRAN_DELETED = FALSE
                )
    GROUP BY 1, 2, 3, 4
),

advance_28 AS (
    SELECT
        adv.advance_id AS credit_id,
        SUM(pay.amount) AS total_collections_by_28
    FROM ANALYTIC_DB.DBT_marts.fct_advances adv
        LEFT JOIN APPLICATION_DB.TRANSACTIONS_DAVE.PAYMENT pay
            ON (adv.advance_id = pay.advance_id
                AND pay.status IN ('COMPLETED')
                AND TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', pay.created::timestamp_ntz)) <=
                    DATEADD(MONTH, 1, DATEFROMPARTS(YEAR(adv.disbursement_ds_pst), MONTH(adv.disbursement_ds_pst), 28))
                AND pay._FIVETRAN_DELETED = FALSE
                )
    GROUP BY 1
),

ec_25 AS (
    SELECT
        dis.overdraft_id AS credit_id,
        dis.disbursement_ds_pst,
        dis.overdraft_amount AS total_principal,
        dis.service_fee_amount + dis.express_fee_amount + dis.tip_amount + dis.overdraft_amount AS total_receivable,
        SUM(sett.amount) AS total_collections_by_25
    FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement dis
        LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT sett
            ON (dis.overdraft_id = sett.overdraft_id
                AND sett.settlement_status_id = 2 --2 stands for COMPLETE settlements
                AND TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sett.created::timestamp_ntz)) <=
                    DATEADD(MONTH, 1, DATEFROMPARTS(YEAR(dis.disbursement_ds_pst), MONTH(disbursement_ds_pst), 25))
                AND sett._FIVETRAN_DELETED = FALSE
                )
    GROUP BY 1, 2, 3, 4
),

ec_28 AS (
    SELECT
        dis.overdraft_id AS credit_id,
        SUM(sett.amount) AS total_collections_by_28
    FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement dis
        LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT sett
            ON (dis.overdraft_id = sett.overdraft_id
                AND sett.settlement_status_id = 2 --2 stands for COMPLETE settlements
                AND TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sett.created::timestamp_ntz)) <=
                    DATEADD(MONTH, 1, DATEFROMPARTS(YEAR(dis.disbursement_ds_pst), MONTH(disbursement_ds_pst), 28))
                AND sett._FIVETRAN_DELETED = FALSE
                )
    GROUP BY 1
),

adv_merged AS (
    SELECT
        advance_25.*,
        advance_28.total_collections_by_28
    FROM advance_25
        JOIN advance_28
            ON advance_25.credit_id = advance_28.credit_id
),

ec_merged AS (
    SELECT
        ec_25.*,
        ec_28.total_collections_by_28
    FROM ec_25
        JOIN ec_28
            ON ec_25.credit_id = ec_28.credit_id

),

union_all AS (
SELECT * FROM adv_merged
UNION ALL
SELECT * FROM ec_merged
)

SELECT
    DATE_TRUNC('MONTH', disbursement_ds_pst) AS month,
    SUM(total_principal) AS "Principal",
    SUM(total_receivable) AS "Principal + Fees + Tip",
    SUM(total_collections_by_25) AS "Collections through 25th",
    SUM(total_collections_by_28) AS "Collections through 28th",
    "Collections through 25th" / "Principal" AS "MCRR (25th)",
    "Collections through 25th" / "Principal + Fees + Tip" AS "MRR (25th)",
    "Collections through 28th" / "Principal" AS "MCRR (28th)",
    "Collections through 28th" / "Principal + Fees + Tip" AS "MRR (28th)"
FROM union_all
GROUP BY 1
ORDER BY 1 DESC