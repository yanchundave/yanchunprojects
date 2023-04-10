SELECT
    A.USER_ID,
    DATE(A.FIRST_BC_TS_PST) AS DATE_OF_REF,
    CASE
        WHEN A.IS_EC_ACCOUNT_30D THEN 1
        ELSE 0
    END AS IF_APPLIED_EC,
    CASE
        WHEN A.IS_APPROVED_30D THEN 1
        ELSE 0
    END AS IF_APPROVED_EC,
    CASE
        WHEN A.IS_ADVANCE_TAKEN_30D THEN 1
        ELSE 0
    END AS IF_DISBURSEMENT_EC,
    chime_log.has_chime_spotme_n30d,
    chime_log.lowest_chime_bal_n30d_amt_usd,
    nonchime_log.nonchime_competitor_funding_txn_n30d_cnt,
    nonchime_log.nonchime_competitor_funding_txn_n30d_amt_usd,
FROM ANALYTIC_DB.DBT_marts.fct_new_user_journey A
LEFT JOIN DBT.ADV_CHURN_MARTS.FCT_CHIME_NEG_BAL chime_log
    ON A.USER_ID = B.USER_ID AND DATE(A.FIRST_BC_TS_PST) = chime_log.REF_DATE
LEFT JOIN DBT.ADV_CHURN_MARTS.FCT_NONCHIME_COMPETITOR_TXN nonchime_log
    ON A.USER_ID = C.USER_ID AND DATE(A.FIRST_BC_TS_PST) = nonchime_log.REF_DATE
WHERE A.IS_EC_ACCOUNT_30D
