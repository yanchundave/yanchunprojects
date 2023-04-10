{{ config(
    materialized = 'incremental',
    pre_hook = [
        "{{ create_adv_competitors_udfs() }}",
        "grant usage on all functions in schema {{ schema }} to role DAVE_DATA_ANALYST",
        "grant usage on all functions in schema {{ schema }} to role MODE_ROLE",
        "grant usage on all functions in schema {{ schema }} to role MODE_ROLE_RO",
        "grant usage on all functions in schema {{ schema }} to role DATASCI_AIRFLOW_ROLE"
    ],
    unique_key = 'date_of_ref',
    incremental_strategy = 'delete+insert',
    cluster_by = ['date_of_ref'],
    tags = ["adv_competitors"]
) }}

-- leave the possibility to do backfill for multiple days --
WITH dates_acc AS (
	SELECT
        CASE WHEN '{{ var("backfill_start_date") }}' = 'default' THEN CURRENT_DATE() - 30
             ELSE CAST('{{ var("backfill_start_date") }}' AS DATE)
        END AS start_date,
        CASE WHEN '{{ var("backfill_end_date") }}' = 'default' THEN CURRENT_DATE() - 1
             ELSE CAST('{{ var("backfill_end_date") }}' AS DATE)
        END AS end_date
),
chime_log AS
(
    SELECT * FROM {{ 'DBT.ADV_CHURN_MARTS.FCT_CHIME_NEG_BAL' if target.name == 'dev' else ref('fct_chime_neg_bal') }}
),
nonchime_log AS
(
    SELECT * FROM {{ 'DBT.ADV_CHURN_MARTS.FCT_NONCHIME_COMPETITOR_TXN' if target.name == 'dev' else ref('fct_nonchime_competitor_txn') }}
),
plaid_data AS
(
    SELECT * FROM {{ source('datastream', 'bank_transaction') }}
),
requests AS
(
    SELECT * FROM {{'ANALYTIC_DB.DBT_MARTS.REQUESTS' if target.name == 'dev' else source('ANALYTIC_DB', 'REQUESTS')}}
),
bank_connection AS
(
    SELECT * FROM {{ 'APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION' if target.name == 'dev' else source('dave', 'BANK_CONNECTION') }}
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
        SUM(requests.is_disbursed) AS is_disbursed_sum
    FROM bank_connection_newuser
    JOIN requests
    ON bank_connection_newuser.user_id = requests.user_id
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
        chime_log.has_chime_spotme_n30d,
        chime_log.lowest_chime_bal_n30d_amt_usd
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
        bank_user_chime.has_chime_spotme_n30d,
        bank_user_chime.lowest_chime_bal_n30d_amt_usd,
        nonchime_log.nonchime_competitor_funding_txn_n30d_cnt,
        nonchime_log.nonchime_competitor_funding_txn_n30d_amt_usd,
        {{ schema }}.UDF_NEW_MEMBER_CATEGORY(bank_user_chime.is_approved_sum, bank_user_chime.is_disbursed_sum) AS new_member_category,
        CASE
            WHEN bank_user_chime.lowest_chime_bal_n30d_amt_usd < 0 THEN 1
            ELSE 0
        END AS if_adv_from_chime,
        CASE
            WHEN nonchime_log.nonchime_competitor_funding_txn_n30d_cnt > 0 THEN 1
            ELSE 0
        END AS if_adv_from_nonchime
    FROM bank_user_chime
    LEFT JOIN nonchime_log
    ON bank_user_chime.user_id = nonchime_log.user_id
        AND bank_user_chime.date_of_ref = nonchime_log.ref_date
)
SELECT * FROM bank_user_nonchime
