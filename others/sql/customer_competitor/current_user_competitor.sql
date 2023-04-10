with userbankconnection as
(
    select
        user_id,
        DATE(FIRST_BC_TS_PST) AS DATE_OF_REF
    FROM ANALYTIC_DB.DBT_marts.fct_new_user_journey
    WHERE FIRST_BC_TS_PST IS NOT NULL
),
user_segment as
(
    SELECT
        USER_ID,
        DATE_OF_INTEREST,
        USER_LABEL
    FROM DBT.ADV_CHURN_MARTS.FCT_ADV_SEGMENT
    WHERE DATE_OF_INTEREST = CURRENT_DATE() - 1
),
chime_log as
(
    SELECT
        USER_ID,
        REF_DATE,
        HAS_CHIME_SPOTME_L30D,
        -1 * LOWEST_CHIME_BAL_L30D_AMT_USD AS LOWEST_CHIME_L30D_AMT
    FROM DBT.ADV_CHURN_MARTS.FCT_CHIME_NEG_BAL
    WHERE REF_DATE = CURRENT_DATE() - 1

),
nonchime_log as
(
    SELECT
        USER_ID,
        REF_DATE,
        nonchime_competitor_funding_txn_l30d_cnt,
        nonchime_competitor_funding_txn_l30d_amt_usd
    FROM DBT.ADV_CHURN_MARTS.FCT_NONCHIME_COMPETITOR_TXN
    WHERE REF_DATE = CURRENT_DATE() - 1

),
SELECT
    userbankconnection.USER_ID,
    userbankconnection.DATE_OF_INTEREST,
    user_segment.USER_LABEL,
    chime_log.HAS_CHIME_SPOTME_L30D,
    chime_log.LOWEST_CHIME_L30D_AMT,
    nonchime_log.nonchime_competitor_funding_txn_l30d_cnt,
    nonchime_log.nonchime_competitor_funding_txn_l30d_amt_usd,
    case
        when chime_log.HAS_CHIME_SPOTME_L30D > 0  then 1
        else 0
    end as if_chime,
    case
        when nonchime_log.nonchime_competitor_funding_txn_l30d_cnt > 0 then 1
        else 0
    end as if_nonchime,
    case
        when chime_log.HAS_CHIME_SPOTME_L30D > 0 or  nonchime_log.nonchime_competitor_funding_txn_l30d_cnt > 0 then 1
        else 0
    end as if_competitor
FROM userbankconnection
LEFT JOIN user_segment
on userbankconnection.USER_ID = user_segment.USER_ID
LEFT JOIN chime_log
on user_segment.USER_ID = chime_log.USER_ID
LEFT JOIN nonchime_log
on user_segment.USER_ID = nonchime_log.USER_ID