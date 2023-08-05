{{ config(
    materialized = 'table'
) }}

WITH

transactions__dd_candidates_flagged AS (

    SELECT * FROM {{ ref('transactions__dd_candidates_flagged') }}

),

transactions__dd_detected AS (

    SELECT * FROM {{ ref('transactions__dd_detected') }}

),

-- END TABLE THAT CAPTURES DD TRANSACTIONS
flag_dd_records_step1 AS (

    SELECT
        transactions__dd_candidates_flagged.transaction_id,
        transactions__dd_candidates_flagged.reference_id,
        transactions__dd_candidates_flagged.short_description,
        transactions__dd_candidates_flagged.description,
        transactions__dd_candidates_flagged.mcc_edited_description,
        transactions__dd_candidates_flagged.mcc_irs_description,
        transactions__dd_candidates_flagged.analysis_category,
	transactions__dd_candidates_flagged.created_ds    AS created_ds,
	transactions__dd_candidates_flagged.created_ts                          AS created_ts,

        TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver',
                                 transactions__dd_candidates_flagged.created_ts::timestamp_ntz))           AS created_ds_mst,
   CONVERT_TIMEZONE('UTC', 'America/Denver',
                                 transactions__dd_candidates_flagged.created_ts::timestamp_ntz)          AS created_ts_mst,

        TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                 transactions__dd_candidates_flagged.created_ts::timestamp_ntz))           AS created_ds_pst,

        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                         transactions__dd_candidates_flagged.created_ts::timestamp_ntz)                    AS created_ts_pst,
     transactions__dd_candidates_flagged.transaction_ds       						   AS transaction_ds,
	transactions__dd_candidates_flagged.transaction_ts               				   AS transaction_ts,

       TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver',
                         transactions__dd_candidates_flagged.transaction_ts::timestamp_ntz))                AS transaction_ds_mst,
        CONVERT_TIMEZONE('UTC', 'America/Denver',
                         transactions__dd_candidates_flagged.transaction_ts::timestamp_ntz)                AS transaction_ts_mst,

        TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                 transactions__dd_candidates_flagged.transaction_ts::timestamp_ntz))       AS transaction_ds_pst,

        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                         transactions__dd_candidates_flagged.transaction_ts::timestamp_ntz)                AS transaction_ts_pst,

       transactions__dd_candidates_flagged.settled_ds                                                      AS settled_ds,
        transactions__dd_candidates_flagged.settled_ts                                                     AS settled_ts,


        TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver',
                         transactions__dd_candidates_flagged.settled_ts::timestamp_ntz))                     AS settled_ds_mst,
        CONVERT_TIMEZONE('UTC', 'America/Denver',
                         transactions__dd_candidates_flagged.settled_ts::timestamp_ntz)                     AS settled_ts_mst,

        TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                                 transactions__dd_candidates_flagged.settled_ts::timestamp_ntz))           AS settled_ds_pst,

        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',
                         transactions__dd_candidates_flagged.settled_ts::timestamp_ntz)                    AS settled_ts_pst,


        transactions__dd_candidates_flagged.transaction_status,
        transactions__dd_candidates_flagged.transaction_amount,
        transactions__dd_candidates_flagged.external_code,
        transactions__dd_candidates_flagged.merchant_category_code,
        transactions__dd_candidates_flagged.digital_wallet_provider,
        transactions__dd_candidates_flagged.digital_wallet_provider_bucket,
        transactions__dd_candidates_flagged.account_id,
        transactions__dd_candidates_flagged.user_id,
        transactions__dd_candidates_flagged.dave_bank_account_status,
        transactions__dd_candidates_flagged.account_created_ds,
        transactions__dd_candidates_flagged.account_created_ds_pst,
        transactions__dd_candidates_flagged.account_created_month_ds,
        transactions__dd_candidates_flagged.account_created_month_ds_pst,
        transactions__dd_candidates_flagged.months_since_account_created,
        transactions__dd_candidates_flagged.funding_source,
        transactions__dd_candidates_flagged.interchange_fee,
        transactions__dd_candidates_flagged.is_virtual,
	transactions__dd_candidates_flagged.is_external_funding 					    AS is_external_funding,
	 transactions__dd_candidates_flagged.funding_flag                                                    AS is_funding_txn,
        transactions__dd_candidates_flagged.customer_spend_flag                                             AS is_spend_txn,
        transactions__dd_candidates_flagged.retail_sale_flag                                                AS is_retail_sale,
        transactions__dd_candidates_flagged.atm_fee_flag                                                    AS is_atm_fee,
        transactions__dd_candidates_flagged.atm_withdrawl_flag                                              AS is_atm_withdrawal,
        transactions__dd_candidates_flagged.revenue_generation_flag                                         AS is_revenue_generating,
        transactions__dd_candidates_flagged.monthly_fee_flag                                                AS is_monthly_fee,
        transactions__dd_candidates_flagged.interchange_revenue_flag                                        AS is_interchange_revenue,
        transactions__dd_candidates_flagged.bank_active_user_flag                                           AS is_bank_active_user,
        transactions__dd_candidates_flagged.is_instant_withdrawal                                           AS is_instant_withdrawal ,
	transactions__dd_candidates_flagged.iw_amount                                                           AS iw_amount,
	transactions__dd_candidates_flagged.iw_fees                                                             AS iw_fees,
	transactions__dd_candidates_flagged.loomis_transaction_id                                               AS loomis_transaction_id,

        -- DIRECT DEPOSIT FIELDS
        transactions__dd_candidates_flagged.is_non_gov_benefit_dd_candidate,
        transactions__dd_candidates_flagged.is_gov_benefit_dd_candidate,

        MAX(CASE
                WHEN transactions__dd_candidates_flagged.funding_flag = 1
                    AND (transactions__dd_candidates_flagged.is_gov_benefit_dd_candidate = 1
                         OR transactions__dd_candidates_flagged.is_non_gov_benefit_dd_candidate = 1)
                    AND TO_DATE(  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',transactions__dd_candidates_flagged.transaction_ts::timestamp_ntz)) >= transactions__dd_detected.min_dd_criteria_met_eval_start_ds
                THEN 1
                ELSE 0
            END)
            OVER(PARTITION BY transactions__dd_candidates_flagged.transaction_id)                                         AS is_direct_deposit_record_candidate,

        MAX(CASE
                WHEN transactions__dd_candidates_flagged.funding_flag = 1
                    AND (transactions__dd_candidates_flagged.is_gov_benefit_dd_candidate = 1
                         OR transactions__dd_candidates_flagged.is_non_gov_benefit_dd_candidate = 1)
                    AND TO_DATE(  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',transactions__dd_candidates_flagged.transaction_ts::timestamp_ntz)) >= transactions__dd_detected.min_dd_criteria_met_eval_start_ds
                    AND TO_DATE(  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles',transactions__dd_candidates_flagged.transaction_ts::timestamp_ntz)) <= transactions__dd_detected.min_dd_criteria_met_ds
                THEN 1
                ELSE 0
            END)
            OVER(PARTITION BY transactions__dd_candidates_flagged.transaction_id)                                         AS is_direct_deposit_record,
        ROW_NUMBER() OVER(PARTITION BY transactions__dd_candidates_flagged.transaction_id
                          ORDER BY transactions__dd_candidates_flagged.transaction_ds ASC)                                AS dedupe

    FROM transactions__dd_candidates_flagged
    LEFT JOIN transactions__dd_detected
        ON transactions__dd_candidates_flagged.user_id = transactions__dd_detected.user_id
        AND transactions__dd_detected.event_trigger_flag = 1

),

final AS (

    SELECT
        transaction_id,
        reference_id,
        account_id,
        dave_bank_account_status,
        account_created_ds,
        account_created_ds_pst,
        account_created_month_ds,
        account_created_month_ds_pst,
        months_since_account_created,
        user_id,
        short_description,
        description,
        mcc_edited_description,
        mcc_irs_description,
        analysis_category,
        transaction_status,
        transaction_amount,
        interchange_fee,
        merchant_category_code,
        external_code,
        digital_wallet_provider,
        digital_wallet_provider_bucket,
        funding_source,
        created_ts,
        created_ds,
        created_ts_mst,
        created_ds_mst,
        created_ts_pst,
        created_ds_pst,
        transaction_ts,
        transaction_ds,
        transaction_ts_mst,
        transaction_ds_mst,
        transaction_ts_pst,
        transaction_ds_pst,
        settled_ts,
        settled_ds,
        settled_ts_mst,
        settled_ds_mst,
        settled_ts_pst,
        settled_ds_pst,
        is_virtual,
	is_external_funding,
        is_funding_txn,
        is_spend_txn,
        is_retail_sale,
        is_atm_fee,
        is_atm_withdrawal,
        is_revenue_generating,
        is_monthly_fee,
        is_interchange_revenue,
        is_bank_active_user,
        -- DIRECT DEPOSIT FIELDS
        is_non_gov_benefit_dd_candidate,
        is_gov_benefit_dd_candidate,
        is_direct_deposit_record_candidate,
        is_direct_deposit_record,
        -- instant withdrawal
       	is_instant_withdrawal,
	    iw_amount,
	   iw_fees,
	   loomis_transaction_id

    FROM flag_dd_records_step1
    WHERE dedupe = 1

)

SELECT * FROM final