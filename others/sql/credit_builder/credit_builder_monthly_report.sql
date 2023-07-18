{{ config(
    materialized = 'view'
) }}

WITH

fct_overdraft_settlement AS (
    -- ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_SETTLEMENT
    SELECT * FROM {{ ref('fct_overdraft_settlement_ta') }}

),

fct_overdraft_disbursement AS (
    -- ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_DISBURSEMENT
    SELECT * FROM {{ ref('fct_overdraft_disbursement') }}

),

bank_banking_account AS (
    -- APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT
    SELECT * FROM {{ source('bank_banking','ACCOUNT') }}

),

bank_banking_account_type AS (
    -- APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_TYPE
    SELECT * FROM {{ source('bank_banking','ACCOUNT_TYPE') }}

),

bank_banking_account_status AS (
    -- APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.ACCOUNT_STATUS
    SELECT * FROM {{ source('bank_banking','ACCOUNT_STATUS') }}

),

overdraft_account AS (
    -- OVERDRAFT.OVERDRAFT_OVERDRAFT.ACCOUNT
    SELECT * FROM {{ source('overdraft', 'account') }}

),

overdraft_overdraft AS (
    -- OVERDRAFT.OVERDRAFT_OVERDRAFT.OVERDRAFT
    SELECT * FROM {{ source('overdraft', 'overdraft') }}

),

bank_banking_account_payment_reference AS (
    -- APPLICATION_DB.GOOGLE_CLOUD_MYSQL_BANK_BANKING.PAYMENT_REFERENCE
    SELECT * FROM {{ source('bank_banking','PAYMENT_REFERENCE') }}

),

dave_user AS (
    -- APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER
    SELECT * FROM {{ source('dave', 'USER') }}
),

chargeback_transactions AS (
    -- ANALYTIC_DB.DBT_MARTS.CHARGEBACK_TRANSACTIONS
    SELECT * FROM {{ ref('chargeback_transactions') }}
),

loomis_transaction AS (
    -- LOOMIS_DB.LOOMIS.TRANSACTION
    SELECT * FROM {{ source('Loomis', 'TRANSACTION') }}
),

fct_advance_approvals AS (
    -- ANALYTIC_DB.DBT_MARTS.FCT_ADVANCE_APPROVALS
    SELECT * FROM {{ ref('fct_advance_approvals') }}

),

reporting_windows AS (
    -- This cte is designated to generate reporting windows
    -- default values are the first and last day of the previous calendar month
    -- can add more months if needed
	SELECT
        DATEADD(MONTH, -1, DATE_TRUNC('Month',CURRENT_DATE())) AS reporting_start_day,
        DATEADD(DAY, -1, DATE_TRUNC('Month',CURRENT_DATE())) AS reporting_end_day
),

extra_cash_settlement AS (

    SELECT
        fct_overdraft_settlement.overdraft_id,
        reporting_windows.reporting_end_day,
        SUM(IFF(fct_overdraft_settlement.settlement_ds_pst <= reporting_windows.reporting_end_day, fct_overdraft_settlement.settlement_amount, 0)) AS settlement_amount,
        MAX(IFF(fct_overdraft_settlement.settlement_ds_pst <= reporting_windows.reporting_end_day, fct_overdraft_settlement.settlement_ds_pst, NULL)) AS Date_Of_Last_Payment,
        SUM(IFF(fct_overdraft_settlement.settlement_ds_pst BETWEEN reporting_windows.reporting_start_day AND reporting_windows.reporting_end_day,
            fct_overdraft_settlement.settlement_amount, 0)) AS settlement_amount_in_reporting_window,
        SUM(IFF(fct_overdraft_settlement.settlement_ds_pst < reporting_windows.reporting_start_day, fct_overdraft_settlement.settlement_amount, 0)) AS settlement_amount_prev_report
    FROM fct_overdraft_settlement
    CROSS JOIN reporting_windows
    GROUP BY 1, 2

),

-- extra cash table cross join with reporting_end_day
extra_cash_disbursement_date AS (

    SELECT
        fct_overdraft_disbursement.user_id,
        fct_overdraft_disbursement.overdraft_id,
        fct_overdraft_disbursement.amount_due,
        fct_overdraft_disbursement.disbursement_ds_pst,
        fct_overdraft_disbursement.settlement_due_ds_pst AS payback_date,
        reporting_windows.reporting_start_day,
        reporting_windows.reporting_end_day,
        IFF(fct_overdraft_disbursement.disbursement_ds_pst BETWEEN reporting_windows.reporting_start_day AND reporting_windows.reporting_end_day,
            fct_overdraft_disbursement.amount_due, 0) AS disbursed_amount_in_reporting_window, -- including tips and fees
        IFF(fct_overdraft_disbursement.disbursement_ds_pst < reporting_windows.reporting_start_day, fct_overdraft_disbursement.amount_due, 0) AS amount_due_prev_report
    FROM fct_overdraft_disbursement
    CROSS JOIN reporting_windows
    WHERE fct_overdraft_disbursement.disbursement_status = 'COMPLETE'
      AND fct_overdraft_disbursement.disbursement_ds_pst <= reporting_windows.reporting_end_day

),

extra_cash_disbursement AS (

    SELECT
        extra_cash_disbursement_date.user_id,
        extra_cash_disbursement_date.overdraft_id,
        extra_cash_disbursement_date.amount_due,
        extra_cash_disbursement_date.disbursement_ds_pst,
        extra_cash_disbursement_date.payback_date,
        -- this is the new due date defined for the credit builder project which is always due_date + 30 days grace period
        DATEADD(days, 30, extra_cash_disbursement_date.payback_date) AS new_due_date,
        extra_cash_disbursement_date.reporting_end_day,
        extra_cash_settlement.settlement_amount,
        FLOOR(extra_cash_disbursement_date.amount_due - COALESCE(extra_cash_settlement.settlement_amount, 0)) AS remaining_balance, -- round down
        extra_cash_settlement.Date_Of_Last_Payment,
        IFF(remaining_balance > 0 AND new_due_date <= extra_cash_disbursement_date.reporting_end_day,
            DATEDIFF(DAY, new_due_date, extra_cash_disbursement_date.reporting_end_day), NULL) AS past_due_days,
        extra_cash_settlement.settlement_amount_in_reporting_window,
        extra_cash_disbursement_date.disbursed_amount_in_reporting_window,

        FLOOR(extra_cash_disbursement_date.amount_due_prev_report - COALESCE(extra_cash_settlement.settlement_amount_prev_report, 0)) AS remaining_balance_prev_report
    FROM extra_cash_disbursement_date
    LEFT JOIN extra_cash_settlement
      ON extra_cash_disbursement_date.overdraft_id = extra_cash_settlement.overdraft_id
      AND extra_cash_disbursement_date.reporting_end_day = extra_cash_settlement.reporting_end_day

),

last_pmt_and_occurrence AS (

    SELECT DISTINCT
        oa.extra_cash_account_id,
        ecd.reporting_end_day,
        MIN(IFF(ecd.remaining_balance > 0 AND ecd.new_due_date <= ecd.reporting_end_day, ecd.new_due_date, NULL)) AS deliquent_date,
        -- The most recent disbursement date for current account
        MAX(ecd.disbursement_ds_pst) AS last_disbursement_date,
        COALESCE(deliquent_date, last_disbursement_date) AS Date_Of_Occurrence,
        MAX(ecd.Date_Of_Last_Payment) AS Date_Of_Last_Payment,
        MAX(ecd.past_due_days) AS past_due_days,
        SUM(COALESCE(ecd.remaining_balance, 0)) AS current_balance, --regardless of past due or not
        SUM(IFF(ecd.past_due_days > 0, ecd.remaining_balance, 0)) AS deliquent_balance, -- past due balance
        SUM(COALESCE(settlement_amount_in_reporting_window, 0)) AS settlement_amount_in_reporting_window,
        SUM(COALESCE(disbursed_amount_in_reporting_window, 0)) AS disbursed_amount_in_reporting_window,
        SUM(COALESCE(remaining_balance_prev_report, 0)) AS remaining_balance_prev_report
    FROM extra_cash_disbursement ecd
    LEFT JOIN overdraft_overdraft oo
        ON ecd.overdraft_id = oo.id
    LEFT JOIN overdraft_account oa
        ON oo.account_id = oa.id
    GROUP BY 1, 2

),

chargeback AS (
    SELECT
        reporting_windows.reporting_end_day,
        oa.extra_cash_account_id,
        SUM(chargeback_transactions.exception_settled_amount) AS chargeback_amount -- negative value
    FROM chargeback_transactions
    LEFT JOIN loomis_transaction loomis_transaction ON chargeback_transactions.original_transaction_id = loomis_transaction.externalid
    LEFT JOIN fct_overdraft_settlement s ON loomis_transaction.referenceid = s.settlement_id
    LEFT JOIN overdraft_overdraft oo ON s.overdraft_id = oo.id
    LEFT JOIN overdraft_account oa ON oo.account_id = oa.id
    CROSS JOIN reporting_windows
    WHERE chargeback_transactions.MID in ('0006', '0008') -- extra cash
        AND chargeback_transactions.dave_internal_cb_status = 'CHARGEBACK - LOSS' -- confirmed chargeback
        AND chargeback_transactions.status_date BETWEEN reporting_windows.reporting_start_day AND reporting_windows.reporting_end_day
    GROUP BY 1,2
),

extra_cash_accounts AS (
    SELECT
        a.dave_user_id AS user_id,
        a.id AS extra_cash_account_id,
        pr.account_number AS Customer_Account_Number, -- hashed account number
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.created::timestamp_ntz)) AS created_ds_pt,

        -- suspended accounts are considered as closed, confirmed by Risk team
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', oa.closed_at::timestamp_ntz)) AS closed_ds_pt,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.updated::timestamp_ntz)) AS updated_ds_pt,
        IFF(acs.code in ('closed', 'suspended'), COALESCE(closed_ds_pt, updated_ds_pt), NULL) AS closed_ds_pt_updated,
        IFF(closed_ds_pt_updated <= dir.reporting_end_day, closed_ds_pt_updated, NULL) AS closed_ds_pt_final, -- only closed before reporting

        dave_user.fraud,
        dir.reporting_end_day

    FROM bank_banking_account a
    INNER JOIN bank_banking_account_type att
        ON a.account_type_id = att.id AND att.name = 'Extra Cash Account'
    LEFT JOIN bank_banking_account_status acs
        ON a.account_status_id = acs.id
    LEFT JOIN bank_banking_account_payment_reference pr
        ON a.id = pr.account_id
    LEFT JOIN overdraft_account oa
        ON a.id = oa.extra_cash_account_id
    LEFT JOIN dave_user ON a.dave_user_id = dave_user.id
    CROSS JOIN reporting_windows dir
    WHERE created_ds_pt <= dir.reporting_end_day -- accounts only open before the last day of the reporting period
        AND NOT a._FIVETRAN_DELETED
        AND NOT att._FIVETRAN_DELETED
        AND NOT acs._FIVETRAN_DELETED
),

all_extra_cash_approvals AS (
    SELECT DISTINCT
        DATE_TRUNC('MONTH', requested_ds_pst) AS requested_month,
        user_id,
        MAX(max_approved_amount) OVER (PARTITION BY requested_month, user_id) AS highest_credit_of_month,
        dir.reporting_end_day
    FROM fct_advance_approvals fa
    CROSS JOIN reporting_windows dir
    WHERE fa.product = 'Extra Cash'
      AND fa.max_approved_amount IS NOT NULL
      AND fa.max_approved_amount != 0
      AND fa.requested_ds_pst <= dir.reporting_end_day
),

credit_all_time_high AS (
    SELECT
        user_id,
        reporting_end_day,
        MAX(highest_credit_of_month) AS Highest_Credit
    FROM all_extra_cash_approvals
    GROUP BY 1, 2
),

credit_last_month_high AS (
    SELECT
        user_id,
        reporting_end_day,
        highest_credit_of_month AS Credit_Limit
    FROM all_extra_cash_approvals
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, reporting_end_day ORDER BY requested_month DESC) = 1
),

accounts_enriched AS (
    SELECT
        ec.user_id AS user_id,
        ec.extra_cash_account_id,
        ec.Customer_Account_Number,
        ec.created_ds_pt,
        lpo.Date_Of_Occurrence,
        lpo.Date_Of_Last_Payment,
        ec.closed_ds_pt_final,
        lpo.past_due_days,
        lpo.current_balance,
        lpo.deliquent_balance,
        COALESCE(lpo.remaining_balance_prev_report, 0) + COALESCE(lpo.disbursed_amount_in_reporting_window, 0) AS monthly_payment, -- previous balance + newly borrowed money in this reporting window
        FLOOR(COALESCE(lpo.settlement_amount_in_reporting_window, 0) + COALESCE(chargeback.chargeback_amount, 0)) AS actual_payment, -- chargeback_amount is negative

        -- status assuming the account is not closed
        CASE WHEN lpo.past_due_days IS NOT NULL AND lpo.past_due_days > 0 -- has past due overdraft
            THEN
                CASE WHEN lpo.past_due_days <= 30 THEN '71'
                    WHEN lpo.past_due_days <= 60 THEN '78'
                    WHEN lpo.past_due_days <= 90 THEN '80'
                    WHEN lpo.past_due_days <= 120 THEN '82'
                    ELSE '97' -- over 120 days
                END
            ELSE '11'
            END AS status_if_not_closed,

        -- final account status with consideration of closed
        CASE WHEN ec.closed_ds_pt_final IS NOT NULL AND ec.closed_ds_pt_final <= ec.reporting_end_day -- account is closed status as of reporting_end_date
            THEN
                CASE WHEN ec.fraud = false -- not due to fraud
                    THEN
                        CASE
                            WHEN DATEDIFF(DAY, ec.created_ds_pt, ec.closed_ds_pt_final) <= 14 -- closed within 14 days since open
                                THEN
                                    CASE
                                        WHEN DATE_TRUNC('Month', DATE(ec.closed_ds_pt_final)) = DATE_TRUNC('Month', DATE(ec.created_ds_pt)) -- closed and open in the same month
                                            THEN NULL
                                        ELSE -- closed and open in the different month
                                            IFF(DATE_TRUNC('Month', DATE(ec.closed_ds_pt_final)) = DATE_TRUNC('Month', DATE(ec.reporting_end_day)), 'DA', NULL) -- closed in reporting month
                                    END
                            ELSE -- closed after 14 days since open
                                CASE
                                    WHEN DATEDIFF(DAY, ec.closed_ds_pt_final, ec.reporting_end_day) <= 60 -- closed for less than or equal 60  days
                                        THEN status_if_not_closed
                                    ELSE -- closed more than 60  days
                                        IFF(DATEDIFF(DAY, ec.closed_ds_pt_final, ec.reporting_end_day) <= 150, '13', NULL) -- only report closed if closed less than 150 days
                                    END
                            END
                    ELSE -- closed due to fraud
                        IFF(DATE_TRUNC('Month', DATE(ec.closed_ds_pt_final)) = DATE_TRUNC('Month', DATE(ec.reporting_end_day)), 'DF', NULL) -- report DF in the closed month
                    END
            ELSE status_if_not_closed -- not closed
            END AS account_status,

        CASE status_if_not_closed
            WHEN '11' THEN '0'
            WHEN '71' THEN '1'
            WHEN '78' THEN '2'
            WHEN '80' THEN '3'
            WHEN '82' THEN '4'
            WHEN '97' THEN 'L'
            ELSE NULL END AS payment_rating_if_closed,

        clm.Credit_Limit,
        cat.Highest_Credit,

        ec.reporting_end_day
    FROM extra_cash_accounts ec
    LEFT JOIN last_pmt_and_occurrence lpo
      ON ec.extra_cash_account_id = lpo.extra_cash_account_id AND ec.reporting_end_day = lpo.reporting_end_day
    LEFT JOIN chargeback
        ON ec.extra_cash_account_id = chargeback.extra_cash_account_id AND ec.reporting_end_day = chargeback.reporting_end_day
    LEFT JOIN credit_last_month_high clm
        ON ec.user_id = clm.user_id AND ec.reporting_end_day = clm.reporting_end_day
    LEFT JOIN credit_all_time_high cat
        ON ec.user_id = cat.user_id AND ec.reporting_end_day = cat.reporting_end_day
    WHERE account_status IS NOT NULL -- NULL will be filtered out
),

final AS (
    SELECT
        user_id,
        Customer_Account_Number,
        'C' AS Portfolio_Type,
        15 AS Account_Type,
        created_ds_pt AS Date_Open,
        DATE(Date_Of_Occurrence) AS Date_Of_Occurrence,
        DATE(Date_Of_Last_Payment) AS Date_Of_Last_Payment,
        IFF(account_status IN ('13', 'DA', 'DF'), closed_ds_pt_final, NULL) AS Date_Closed,
        account_status AS Account_Status,
        IFF(account_status IN ('13', 'DA', 'DF'), payment_rating_if_closed, NULL) AS Payment_Rating,
        IFF(account_status IN ('13', 'DA', 'DF'), 'XA', NULL) AS Compliance_Condition_Code,
        Credit_Limit,
        Highest_Credit,
        COALESCE(current_balance, 0) AS Current_Balance,
        COALESCE(deliquent_balance, 0) AS Amount_Past_Due,
        FLOOR(monthly_payment) AS Monthly_Payment, --round down to whole amount
        IFF(actual_payment > 0, actual_payment, 0) AS Actual_Payment, -- cannot be negative
        'P' AS Terms_Frequency,
        '001' AS Terms,
        IFF(account_status = '97', current_balance, NULL) AS Original_Charge_Off_Amount,
        reporting_end_day AS Date_Of_Account_Information
    FROM accounts_enriched
)

SELECT * FROM final