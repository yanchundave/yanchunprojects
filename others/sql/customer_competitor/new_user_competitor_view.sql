

CREATE OR REPLACE TABLE DBT.DEV_YANCHUN_PUBLIC.competitor_new_member AS

WITH dates_acc AS (
	SELECT
        DATE('2023-01-01') AS start_date,
        CURRENT_DATE() - 1 AS end_date
),
chime_log AS
(
    SELECT * FROM DBT.ADV_CHURN_MARTS.FCT_CHIME_NEG_BAL
),
nonchime_log AS
(
    SELECT * FROM DBT.ADV_CHURN_MARTS.FCT_NONCHIME_COMPETITOR_TXN
),
plaid_data AS
(
    SELECT * FROM datastream_prd.dave.bank_transaction
),
requests AS
(
    SELECT * FROM ANALYTIC_DB.DBT_MARTS.REQUESTS
),
disbursement AS
(
    SELECT * FROM ANALYTIC_DB.DBT_MARTS.disbursements
),
bank_connection AS
(
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION
),
bank_connection_newuser AS
(
    SELECT
        user_id,
        MIN(DATE(initial_pull)) AS date_of_ref
    FROM bank_connection
    CROSS JOIN dates_acc
    WHERE initial_pull BETWEEN dates_acc.start_date AND dates_acc.end_date
    GROUP BY user_id
),
bank_user_dave AS
(
    SELECT
        bank_connection_newuser.user_id,
        bank_connection_newuser.date_of_ref,
        SUM(requests.is_approved) AS is_approved_sum,
        SUM(requests.is_disbursed) AS is_disbursed_sum,
        SUM(requests.max_approved_amount) AS dave_approved_amt,
        SUM(disbursement.taken_amount) AS dave_amt,
        AVG(disbursement.taken_amount) AS dave_avg_amt
    FROM bank_connection_newuser
    JOIN requests
    ON bank_connection_newuser.user_id = requests.user_id
    LEFT JOIN disbursement
    ON requests.advance_id = disbursement.advance_id
    WHERE requests.requested_ds_pst BETWEEN  bank_connection_newuser.date_of_ref AND bank_connection_newuser.date_of_ref +  interval '30 days'
    GROUP BY 1, 2
),
bank_user_chime AS
(
    SELECT
        bank_user_dave.user_id,
        bank_user_dave.date_of_ref,
        bank_user_dave.is_approved_sum,
        bank_user_dave.is_disbursed_sum,
        bank_user_dave.dave_approved_amt,
        bank_user_dave.dave_amt,
        bank_user_dave.dave_avg_amt,
        chime_log.has_chime_spotme_n30d,
        1 AS chime_count,
        -1* chime_log.lowest_chime_bal_n30d_amt_usd AS lowest_chime_n30d
    FROM bank_user_dave
    LEFT JOIN chime_log
    ON bank_user_dave.user_id = chime_log.user_id
        AND bank_user_dave.date_of_ref = chime_log.ref_date
),
bank_user_nonchime AS
(
    SELECT
        bank_user_chime.user_id,
        bank_user_chime.date_of_ref,
        bank_user_chime.is_approved_sum,
        bank_user_chime.is_disbursed_sum,
        bank_user_chime.dave_approved_amt,
        bank_user_chime.dave_amt,
        bank_user_chime.dave_avg_amt,
        bank_user_chime.has_chime_spotme_n30d,
        bank_user_chime.lowest_chime_n30d,
        bank_user_chime.chime_count,
        nonchime_log.nonchime_competitor_funding_txn_n30d_cnt,
        nonchime_log.nonchime_competitor_funding_txn_n30d_amt_usd,
        CASE
          WHEN bank_user_chime.is_approved_sum = 0 THEN 'not_approved'
          WHEN bank_user_chime.is_disbursed_sum = 0 THEN 'not_taken'
          ELSE 'taken'
        END AS new_member_category,
        CASE
            WHEN bank_user_chime.lowest_chime_n30d > 0.01 THEN 1
            ELSE 0
        END AS if_adv_from_chime,
        CASE
            WHEN nonchime_log.nonchime_competitor_funding_txn_n30d_amt_usd > 0.01 THEN 1
            ELSE 0
        END AS if_adv_from_nonchime,
        CASE
          WHEN bank_user_chime.lowest_chime_n30d > 0.01 OR nonchime_log.nonchime_competitor_funding_txn_n30d_amt_usd > 0.01 THEN 1
          ELSE 0
        END AS if_competitor,
        CASE
          WHEN dave_amt > 0 then 1
          else 0
        END AS if_dave_taken
    FROM bank_user_chime
    LEFT JOIN nonchime_log
    ON bank_user_chime.user_id = nonchime_log.user_id
        AND bank_user_chime.date_of_ref = nonchime_log.ref_date
)
SELECT * FROM bank_user_nonchime