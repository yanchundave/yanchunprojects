{{ config(
    materialized = 'table',
) }}

WITH

transactions AS (

    SELECT * FROM {{ source('bank_banking', 'TRANSACTION') }}

),

payment_reference AS (

    SELECT * FROM {{ source('bank_banking', 'PAYMENT_REFERENCE') }}

),

dim_spending_accounts AS (


    SELECT * FROM {{ ref('dim_spending_accounts') }}

),

dim_merchant_category_code AS (

    SELECT * FROM {{ ref('dim_merchant_category_code') }}

),

dim_cards AS (

    SELECT * FROM {{ ref('dim_cards') }}

),

account_type AS (

    SELECT * FROM {{ source('bank_banking', 'ACCOUNT_TYPE') }}

),

account AS (

    SELECT * FROM {{ source('bank_banking', 'ACCOUNT') }}

),

instant_withdrawal_transaction_adjustment as (
	select * from {{ ref('instant_withdrawal_transaction_adjustment') }}
),

prn_virtual_lu AS (

    SELECT
        payment_reference_id,
        is_virtual,
        ROW_NUMBER() OVER (PARTITION BY payment_reference_id ORDER BY card_created_ds DESC) AS rn_desc
    FROM dim_cards

),

checking_account_ids AS (

    SELECT
        account.id AS account_id

    FROM account
    INNER JOIN account_type
        ON account.account_type_id = account_type.id
    WHERE account_type.code = 'CHECKING'

),

checking_payment_reference AS (

  SELECT *
  FROM payment_reference
  WHERE account_id IN (SELECT * FROM checking_account_ids)

),

settled_transactions AS (

    SELECT
        transactions.id                                                                                         AS transaction_id,
	transactions.reference_id										as reference_id,
        checking_payment_reference.account_id,
        dim_spending_accounts.user_id,
        dim_spending_accounts.dave_bank_account_status,
      	dim_spending_accounts.account_created_ds,
        dim_spending_accounts.account_created_ds_pst,
      	LAST_DAY(dim_spending_accounts.account_created_ds)                                                      AS account_created_month_ds,
        LAST_DAY(dim_spending_accounts.account_created_ds_pst)                                                  AS account_created_month_ds_pst,

        CASE
            WHEN (UPPER(REPLACE(transactions.external_code,' ','')) LIKE 'SD%')
              THEN DATEDIFF(month, dim_spending_accounts.account_created_ts, DATEADD(day, -1, transactions.settled_at))
            ELSE DATEDIFF(month, dim_spending_accounts.account_created_ts, transactions.SETTLED_AT)
      	END                                                                                                     AS months_since_account_created,

        transactions.short_description,
        transactions.description,
        transactions.status                                                                                     AS transaction_status,
        transactions.amount                                                                                     AS transaction_amount,

        -- Performing this change as per instructions from Galileo.
        -- Galileo will at some point perform this change. They have agreed to notify us
        CASE
            WHEN TRIM(transactions.external_code) = 'ADC'
                THEN (-1 * transactions.interchange_fee)
            ELSE transactions.interchange_fee
        END                                                                                                     AS interchange_fee,

        transactions.merchant_category_code,
        TRIM(transactions.external_code)                                                                        AS external_code,
        dim_merchant_category_code.edited_description                                                           AS mcc_edited_description,
        dim_merchant_category_code.irs_description                                                              AS mcc_irs_description,
        dim_merchant_category_code.analysis_category,
        transactions.digital_wallet_provider,

        CASE
            WHEN transactions.digital_wallet_provider IS NULL AND transactions.settled_at < '2020-12-17'
              THEN '00. TRACKING UNAVAILABLE'
            WHEN transactions.digital_wallet_provider IS NULL                                           THEN  '01. NON_DIGITAL'
            WHEN transactions.digital_wallet_provider='Google Inc.'                                     THEN  '02. GOOGLE'
            WHEN transactions.digital_wallet_provider='Apple Inc.'                                      THEN  '03. APPLE'
            WHEN transactions.digital_wallet_provider IS NOT NULL                                       THEN  '04. DIGITAL_OTHER'
            ELSE                                                                                             'UNKNOWN'
        END                                                                                                     AS digital_wallet_provider_bucket,

        prn_virtual_lu.is_virtual,
        transactions.transacted_at                                                                              AS transaction_ts,
        TO_DATE(transactions.transacted_at)                                                                     AS transaction_ds,

        CASE
            WHEN TRIM(transactions.external_code) LIKE 'SD%'
                THEN DATEADD('day', -1, transactions.settled_at)
            ELSE transactions.settled_at
        END                                                                                                     AS settled_ts,

        CASE
            WHEN TRIM(transactions.external_code) LIKE 'SD%'
                THEN DATEADD('day', -1, date(transactions.settled_at))
            ELSE TO_DATE(transactions.settled_at)
        END                                                                                                     AS settled_ds,

        transactions.created                                                                                    AS created_ts,
        TO_DATE(transactions.created)                                                                           AS created_ds,
        -- FUNDING FLAG
        CASE
            WHEN transactions.amount > 0

                -- THE LATTER PORTION OF EXTERNAL CODES MIGHT REPRESENT LOADS IN THE FUTURE
                AND (TRIM(transactions.external_code) LIKE 'PM%' OR TRIM(transactions.external_code) IN ('SE28', 'SDT', 'MPT'))
                THEN 1
            ELSE 0
        END                                                                                                     AS funding_flag,
        -- FUNDING SOURCE
        CASE
            WHEN funding_flag=0
              THEN 'Non-Funding Transaction'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMTP' AND transactions.description='Google Pay Transfer'
            	THEN transactions.external_code || ': ' || 'Google Pay'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMTP' AND transactions.description='Apple Pay Transfer'
              THEN transactions.external_code || ': ' || 'Apple Pay'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMTP' AND transactions.description='Debit Card Transfer'
              THEN transactions.external_code || ': ' || 'Debit Card'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMEH'
              THEN transactions.external_code || ': ' || 'Express Advances [Internal]'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMSH [Internal]'
              THEN transactions.external_code || ': ' || 'Standard Advances [Internal]'
	    WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMC2' AND transactions.description='Extra Cash advance'
              THEN transactions.external_code || ': ' || 'Extra Cash Advances [Internal]'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMC2' AND transactions.description!='Extra Cash advance'
              THEN transactions.external_code || ': ' || 'Goal Transfer [Internal]'
	    WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMWR'
              THEN transactions.external_code || ': ' || 'Dave Surveys [Internal]'
	   WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMC1'
              THEN transactions.external_code || ': ' || 'Dave Rewards [Internal]'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMAC'
              THEN transactions.external_code || ': ' || 'ACH Pull from App'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMEV'
              THEN transactions.external_code || ': ' || 'ACH Transfer to Evolve'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND UPPER(transactions.description) LIKE '% INGO %'
              THEN transactions.external_code || ': ' || 'Ingo Check'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND transactions.description='Branch Messenger - Deposit Account'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%stripe%'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%postmates%'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%doordash%'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%inc. %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%inc %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%inc %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%corp %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%pay %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%payout %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%instant %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX' AND LOWER(transactions.description) LIKE '%deposit account%' AND LOWER(transactions.description) LIKE '%earnin %'
              THEN transactions.external_code || ': ' || 'Deposits'
            WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMMX'
              THEN transactions.external_code || ': ' || 'Other'
            WHEN UPPER(REPLACE(transactions.external_code,' ','')) IN ('PMGO','PMGT')
              THEN transactions.external_code || ': ' || 'Green Dot'
	   WHEN UPPER(REPLACE(transactions.external_code,' ',''))= 'PMVH'
		THEN 'PMVH'
            ELSE 'Other'


	      END                                                                                                     AS funding_source,



        -- CUSTOMER SPEND
        CASE
            WHEN transactions.amount < 0

                -- EXCLUDE ALL ATM WITHDRAWALS
                AND TRIM(transactions.external_code) NOT IN ('SE8', 'SE9', 'SDY', 'SDW', 'MPY', 'MPW')

                AND ((TRIM(transactions.external_code) LIKE 'SE%')
                     OR (TRIM(transactions.external_code) LIKE 'SD%')
                     OR (TRIM(transactions.external_code) LIKE 'IS%')
                     OR (TRIM(transactions.external_code) LIKE 'MP%')
                     -- Removed electronic and paper bill pay b/c of disparity w/ Galileo Dashboard
                     -- OR (TRIM(transactions.external_code) IN ('ADZ', 'ADR'))
                    )
            THEN 1
            ELSE 0
        END                                                                                                     AS customer_spend_flag,

        -- RETAIL SALES
        CASE
            WHEN transactions.amount < 0
                AND TRIM(transactions.external_code) IN ('SE5', 'SDA', 'SDN', 'SDP', 'MPA', 'MPN', 'MPP', 'ISA')
            THEN 1
            ELSE 0
        END                                                                                                     AS retail_sale_flag,

        -- ATM FEE
        CASE
            WHEN TRIM(transactions.external_code) IN ('FE0014', 'FE0013')
            THEN 1
            ELSE 0
        END                                                                                                     AS atm_fee_flag,

        -- ATM WITHDRAWALS
        CASE
            WHEN TRIM(transactions.external_code) IN ('SE9', 'SDY', 'SDW', 'MPY', 'MPW')
            THEN 1
            ELSE 0
        END                                                                                                     AS atm_withdrawl_flag,

        -- REVENUE-GENERATING EVENTS FOR DAVE
        -- NOT INCLUDING ADJUSTMENTS AT THE TIME
        CASE
            WHEN (
                  (transactions.interchange_fee > 0
                   AND (

                        -- MasterCard
                        TRIM(transactions.external_code) LIKE 'SE%'

                        -- Moneypass - CURRENTLY NOT RECEIVING RECORDS BUT MIGHT IN FUTURE
                        OR TRIM(transactions.external_code) LIKE 'MP%'

                        -- Interlink/VISA
                        OR TRIM(transactions.external_code) LIKE 'IS%'

                        -- Maestro (These have a 1 day lag)
                        OR TRIM(transactions.external_code) LIKE 'SD%'

                        -- Mastercard Adjustments
                        -- OR TRIM(transactions.external_code) = 'ADC'
                       )
                  )

                  -- Includes domestic and international atm fees as well as monthly subscription fees
                  OR (TRIM(transactions.external_code) LIKE 'FE%' AND amount < 0)
                 )
                THEN 1
            ELSE 0
        END                                                                                                    AS revenue_generation_flag,

        -- MONTHLY FEES
        CASE
            WHEN TRIM(transactions.external_code) = 'FE0203'
                AND COALESCE(transactions.amount, 0) < 0
                THEN 1
            ELSE 0
        END                                                                                                    AS monthly_fee_flag,

        -- INTERCHANGE REVENUE FEES
        CASE
            WHEN (
                  (transactions.interchange_fee <> 0

                   -- EXCLUDE ALL ATM WITHDRAWALS AS PER ANURAG
                   AND TRIM(transactions.external_code) NOT IN ('SE9', 'SDY', 'SDW', 'MPY', 'MPW')
                   AND (

                        -- MasterCard
                        TRIM(transactions.external_code) LIKE 'SE%'

                        -- Moneypass - CURRENTLY NOT RECEIVING RECORDS BUT MIGHT IN FUTURE
                        OR TRIM(transactions.external_code) LIKE 'MP%'

                        -- Interlink/VISA
                        OR TRIM(transactions.external_code) LIKE 'IS%'

                        -- Maestro (These have a 1 day lag)
                        OR TRIM(transactions.external_code) LIKE 'SD%'

                        -- Mastercard Adjustments
                        OR TRIM(transactions.external_code) = 'ADC'
                       )
                  )
                 )
                THEN 1
            ELSE 0
        END                                                                                                     AS interchange_revenue_flag,

	case
		when  funding_flag=1
			and funding_source not ilike '%[Internal]%'
			and transactions.amount>1
		then 1 else 0 end									    AS is_external_funding,

        -- BANK ACTIVE USER
        CASE
            WHEN transactions.amount < 0
                AND
		(revenue_generation_flag=1 or customer_spend_flag=1) -- revenue generating or spend flag, does not include IW
                THEN 1
            WHEN is_external_funding=1
		THEN 1
            ELSE 0
        END                                                                                                     AS bank_active_user_flag,

        DENSE_RANK()
                OVER (PARTITION BY funding_flag, transactions.description, transactions.external_code
                      ORDER BY dim_spending_accounts.user_id ASC)
        + DENSE_RANK()
                OVER (PARTITION BY funding_flag, transactions.description, transactions.external_code
                      ORDER BY dim_spending_accounts.user_id DESC)
        - 1                                                                                                     AS unique_users_by_desc_external_code_funding,
	-- bring in Instant withdrawal from loomis
	CASE when transactions.external_code='ADcx' THEN 1 else 0 end                                           AS is_instant_withdrawal,
	abs(
	CASE WHEN instant_withdrawal_transaction_adjustment.loomis_transaction_id IS not NULL
	    THEN instant_withdrawal_transaction_adjustment.loomis_amount
	when transactions.external_code='ADcx' and loomis_created_ts::date<='2022-09-24'
		then transactions.amount/1.015
		else null end
	)                                                                                                       AS IW_amount,
	--IW FEES ARE TECHNICALLY REVENUE GENERATING
	ABS(
	CASE WHEN instant_withdrawal_transaction_adjustment.loomis_transaction_id IS not NULL THEN LOOMIS_FEES
	when transactions.external_code='ADcx' and loomis_created_ts::date<='2022-09-24'
		then transactions.Amount-(transactions.amount/1.015)
        else NULL end
	)                       							                        AS IW_fees,
	-- if this null and row is IW  then that means the iw amount and fee are hard coded
	instant_withdrawal_transaction_adjustment.loomis_transaction_id                                         AS loomis_transaction_id

    FROM transactions
    INNER JOIN checking_payment_reference
        ON transactions.payment_reference_id = checking_payment_reference.id
    LEFT JOIN dim_spending_accounts
        ON checking_payment_reference.account_id = dim_spending_accounts.account_id
    LEFT JOIN dim_merchant_category_code
        ON transactions.merchant_category_code = dim_merchant_category_code.merchant_category_code
    LEFT JOIN prn_virtual_lu
        ON transactions.payment_reference_id = prn_virtual_lu.payment_reference_id AND prn_virtual_lu.rn_desc = 1
    left join instant_withdrawal_transaction_adjustment
	on transactions.external_code='ADcx'
	and transactions.reference_id::string =instant_withdrawal_transaction_adjustment.transaction_adjustment_id::string
    WHERE transactions.status = 'settled'
  --Excludes rows that are deleted but not archived
   and (transactions._FIVETRAN_DELETED=True and transactions.is_archived=False)=False

),

final AS (

    SELECT
        transaction_id,
	reference_id,
        account_id,
        user_id,
        dave_bank_account_status,
        account_created_ds,
        account_created_ds_pst,
        account_created_month_ds,
        account_created_month_ds_pst,
        months_since_account_created,
        short_description,
        description,
        created_ds,
        created_ts,
        transaction_ds,
        transaction_ts,
        settled_ds,
        settled_ts,
        transaction_status,
        transaction_amount,
        external_code,
        merchant_category_code,
        mcc_edited_description,
        mcc_irs_description,
        analysis_category,
        digital_wallet_provider,
        digital_wallet_provider_bucket,
        is_virtual,
        interchange_fee,
        funding_source,
        funding_flag,
        customer_spend_flag,
        retail_sale_flag,
        atm_fee_flag,
        atm_withdrawl_flag,
        revenue_generation_flag,
        monthly_fee_flag,
        interchange_revenue_flag,
	is_external_funding,
        bank_active_user_flag,
        unique_users_by_desc_external_code_funding,
	is_instant_withdrawal,
	iw_amount,
	iw_fees,
	loomis_transaction_id,

        -- LOGIC THAT FLAGS CANDIDATE RECORDS FOR DIRECT DEPOSIT LOGIC RELATED TO GOVERNMENT BENEFITS
        -- SPECIFIC LOGIC CARVED OUT FOR PMEV
        CASE
            WHEN funding_flag = 1
                AND external_code = 'PMEV'
                AND (
                     description ILIKE '%DUA%'
                     OR description ILIKE '%PUA%'
                     OR description ILIKE '%UI-%'
                     OR description ILIKE '%UI %'
                     OR description ILIKE '%UI,%'
                     OR description ILIKE '%UI%PAY%'
                     OR description ILIKE '%UI'
                     OR description ILIKE '%BEN%UI'
                     OR description ILIKE '%UI%BEN'
                     OR description ILIKE '%UNEM%'
                     OR description ILIKE '%UEMPLOY%'
                     OR description ILIKE '%STATE%BENEF%'
                     OR description ILIKE '%310 %'
                     OR description ILIKE '%310,%'
                     OR description ILIKE '%310'
                     OR description ILIKE '%DFAS%'
                     OR description ILIKE '%DOL %'
                     OR description ILIKE '%DOL'

                     -- HAVE ONE BAD RECORD WITH STRIPE ON IT
                     OR description ILIKE '%DOL,%'

                     OR description ILIKE '%IDES%PAYM%'
                     OR description ILIKE '%COMM%PA%BENE%'
                     OR description ILIKE '%MODES,%'
                     OR description ILIKE '%IHSS%'
                     OR description ILIKE '%OTDA%'
                     OR description ILIKE '%ADWS%'
                     OR description ILIKE '%VEC%BENE%'
                     OR description ILIKE '%LWA%'
                     OR description ILIKE '%NM DWS%'
                     OR description ILIKE '%CARES%'
                     OR description ILIKE '%FEMA%'
                     OR description ILIKE '%DCSE%'
                     OR description ILIKE '%TCS%449%'
                     OR description ILIKE '%STA%TREAS%'
                     OR description ILIKE '%HEALTH HUMAN SVC%'
                     OR description ILIKE '%DC%SYS%DIST%'
                     OR description ILIKE '%ODJFS%'
                     OR description ILIKE '%STATE%WISC%SSI'
                     OR description ILIKE '%TREHREMPL%'
                     OR description ILIKE '%NHUC BEN%'
                     OR description ILIKE '%EMPLOYMENT PLUS%'
                     OR description ILIKE '%BENEFIT PAYMENTS%'
                     OR description = 'WIS.DCF/W-2, W2 PAYMENT'
                     OR description = '3801000000000000, FED PAYMNT'
                     OR description ILIKE '%LA REV%COVID%'
                     OR description ILIKE '%vermont det%'
                     OR description ILIKE '%NEVADA ESD%'
                     OR description ILIKE '%PAID%LEAVE%BENE%'
                     OR description ILIKE '%PUB%EMP%RET%BEN%'
                    )
                AND NOT (
                         description ILIKE '%payroll%'
                         OR description ILIKE '%fed%sal%'
                         OR description ILIKE '%ref%adv%'
                         OR description ILIKE '%brigit%'
                         OR description ILIKE '%digit.co%'
                         OR description ILIKE '%royalty%taxes%'
                         OR description ILIKE '%suny%'
                         OR description ILIKE '%taxman%holdings%'
                         OR description ILIKE '%jth%'
                         OR description ILIKE '%centax%'
                         OR description ILIKE '%walmart%'
                         OR description ILIKE '%airfreight%'
                         OR description ILIKE '%hometax%'
                         OR description ILIKE '%liberty%tax%'
                         OR description ILIKE '%import%'
                         OR description ILIKE '%child%sup%'

                         -- MILITARY SALARY
                         OR description ILIKE '%DFAS%SAL%'

                         -- ALLOTMENTS ARE DEDUCTIONS ON PAYCHECK INSTALLMENTS THAT ARE PAID OUT ONCE REACH A CERTAIN transaction_amount
                         OR description ILIKE '%DFAS%ALLOT%'

                         OR description ILIKE '%DFAS%ALLT'

                         -- ARMY ACTIVE COMBAT TRAINING
                         OR description ILIKE '%DFAS%ARMY%ACT%'

                         -- NAVY ACTIVE COMBAT TRAINING
                         OR description ILIKE '%DFAS%NAVY%ACT%'

                         -- MILITARY RESERVE COMPONENT
                         OR description ILIKE '%DFAS%ARMY%RC%'

                         -- MOST LIKELY TAX RELATED
                         OR description ILIKE '%DFAS%DEDUCTION%'

                         -- AIR FORCE PAY
                         OR description ILIKE '%DFAS%AF%PAY%'

                         -- NAVY RESERVE
                         OR description ILIKE '%DFAS%NAVY%RES%'

                         OR description ILIKE '%truist%'
                         OR description ILIKE '%quiktrip%'
                         OR description ILIKE '%square%'
                         OR description ILIKE '%railway%'
                         OR description ILIKE 'american bui%'
                         OR description ILIKE '%quickbooks%'
                         OR description ILIKE 'guilford%'
                         OR description ILIKE '%individual%'
                         OR description ILIKE 'quick credit%'
                         OR description ILIKE 'caresource%'
                         OR description ILIKE 'paychex%'
                         OR description ILIKE 'dayton north%'
                         OR description ILIKE 'crb%upgrade%'
                         OR description ILIKE 'oregon cares fun%'
                         OR description ILIKE 'lenovo%settle%'
                         OR description ILIKE 'albert savings%'
                         OR description ILIKE 'stride bank%'
                         OR description ILIKE 'bakkt%'
                         OR description ILIKE 'credit karma%'
                         OR description ILIKE 'floatme%'
                         OR description ILIKE '%p2p%'
                         OR description ILIKE '%lend%'
                         OR description ILIKE 'progressive%'
                         OR description ILIKE 'equinox%'
                         OR description ILIKE 'voyager%'
                         OR description ILIKE 'payactiv%'
                         OR description ILIKE 'chime%'
                         OR description ILIKE '%stripe%'
                         OR description ILIKE '%liqui%'
                         OR description ILIKE '%drafthou%'
                         OR description ILIKE '%empower%'
                         OR description ILIKE 'safe fleet%'
                         OR description ILIKE '%varo%'
                         OR description ILIKE '%buil%'
                         OR description ILIKE '%equi%'
                         OR description ILIKE '%iqui%'
                         OR description ILIKE '%acqui%'
                         OR description ILIKE '%graduate%'
                         OR description ILIKE '%doordash%'
                         OR description ILIKE 'mt airy%'
                         OR description ILIKE '%whataburger%'
                         OR description ILIKE '%courtney cares%'
                         OR description ILIKE '%godman%gui%'
                         OR description ILIKE '%izqui%'
                         OR description ILIKE '%AMZNUI%'
                         OR description ILIKE '%bank%america%'
                         OR description ILIKE '%fide%inves%'
                         OR description ILIKE '%310 stewart%'
                         OR description ILIKE '%apple%cash%'
                         OR description ILIKE '%bulwark%'
                         OR description ILIKE '%quality private%'
                         OR description ILIKE '%always best care%'
                         OR description ILIKE '%intuit%'
                         OR description ILIKE '%quick%'
                         OR description ILIKE '%recruit%'
                         OR description ILIKE 'rui manage%'
                         OR description ILIKE '%AMZNGH%'
                         OR description ILIKE '%USAA%'
                         OR description ILIKE '%TAX%REF%'
                         OR description ILIKE '%TAX%EFT%'
                         OR description ILIKE '%HEALTH%HUMAN%SALARY%'
                         OR description ILIKE '%TREAS%449%XXSOC%SEC%'
                         OR description ILIKE '%LOUISIANA%STATE%PAYMENT%'
                         OR description ILIKE '%RESOURCE%MGMT%310%'
                         OR description ILIKE '%DIANE%CARES%'
                        )
                THEN 1
            ELSE 0
        END                                                                AS is_gov_benefit_dd_candidate,

        -- LOGIC THAT FLAGS CANDIDATE RECORDS FOR DIRECT DEPOSIT LOGIC - NOT GOVERNMENT BENEFITS
        CASE
            WHEN funding_flag = 1
                AND external_code = 'PMEV'
                AND NOT (

                         -- REMOVING ALL LOANS / ADVANCES /
                         description ILIKE '%brigit%'
                         OR description ILIKE '%digit.co%'
                         OR description ILIKE '%digit%funds%xfer%'
                         OR description ILIKE '%moneylion%'
                         OR description ILIKE '%chime%'
                         OR description ILIKE '%ui ben%'
                         OR description ILIKE '%sigma solutions%'
                         OR description ILIKE '%loan'
                         OR description ILIKE '%loans'
                         OR description ILIKE '%loan funds'
                         OR description ILIKE '%spotloan%'
                         OR description ILIKE '%ace%cash%express%'
                         OR description ILIKE '%fig%loan%'
                         OR description ILIKE '%loan trans%'
                         OR description ILIKE '%zocaloans%'
                         OR description ILIKE '%cash link usa%'
                         OR description ILIKE '%loan ach'
                         OR description ILIKE '%loans pay'
                         OR description ILIKE '%greenline%loans%'
                         OR description ILIKE '%simple fast loan%'
                         OR description ILIKE '%sunshineloans%'
                         OR description ILIKE '%vbs%loan%'
                         OR description ILIKE '%vbs%lend%'
                         OR description ILIKE '%vbs%mint%'
                         OR description ILIKE '%vbs%eagle%'
                         OR description ILIKE '%vbs%clearline%'
                         OR description ILIKE '%Cash 1-802%'
                         OR description ILIKE '%mountain summit%'
                         OR description ILIKE '%loandepot%'
                         OR description ILIKE '%green%arrow%loan%'
                         OR description ILIKE '%big picture loan%'
                         OR description ILIKE '%loan at last%'
                         OR description ILIKE '%deer%ridge%loan%'
                         OR description ILIKE '%community loan%'
                         OR description ILIKE '%good fast loan%'
                         OR description ILIKE '%loan%credit%'
                         OR description ILIKE '%loan%disbur%'
                         OR description ILIKE '%loan by phone%'
                         OR description ILIKE '%wise loan%'
                         OR description ILIKE '%BRIDGE%LEND%'
                         OR description ILIKE '%WESTSIDE%LEND%'
                         OR description ILIKE '%BRIGHTLEND%'
                         OR description ILIKE '%loans-pers%'
                         OR description ILIKE '%clear line loans%'
                         OR description ILIKE '%hispanic loan%'
                         OR description ILIKE '%quick help loan%'
                         OR description ILIKE '%loan adv%'
                         OR description ILIKE '%zillow home loan%'
                         OR description ILIKE '%blue frog loan%'
                         OR description ILIKE '%refund adv%'
                         OR description ILIKE '%capitalgoodfund%'
                         OR description ILIKE '%moves us finan%'
                         OR description ILIKE '%voya fin%'
                         OR description ILIKE '%saverlife%'
                         OR description ILIKE '%prizepool%'
                         OR description ILIKE '%cash%adv%'
                         OR description ILIKE '%jthf%'
                         OR description ILIKE '%advance'
                         OR description ILIKE '%advance, PR%'
                         OR description ILIKE 'paychex%advance%'
                         OR description ILIKE '%vbs%adv%'
                         OR description ILIKE '%check advance us%'
                         OR description ILIKE '%cleo ai%'
                         OR description ILIKE '%cleo, credit%'
                         OR description ILIKE '%klover%'
                         OR description ILIKE '%check%go%'
                         OR description ILIKE '%vola%'
                         OR description ILIKE '%cashnetusa%'
                         OR description ILIKE '%lendly%'
                         OR description ILIKE '%net pay advance%'
                         OR description ILIKE 'awl%'
                         OR description ILIKE '%helixfi%'
                         OR description ILIKE '%floatme%'
                         OR description ILIKE '%albert%instant%'
                         OR description ILIKE '%earninactive%credit%'
                         OR description ILIKE '%earnin%tip%'
                         OR description ILIKE '%earninactive%verify%'
                         OR description ILIKE '%credit sesame%'
                         OR description ILIKE '%covington credit%'
                         OR description ILIKE '%status money inc%'
                         OR description ILIKE '%cash aisle%'
                         OR description ILIKE '%eagle%adv%'
                         OR description ILIKE '%empower inc%'
                         OR description ILIKE '%advance america%'
                         OR description ILIKE '%speedy%funding'
                         OR description ILIKE 'american lending%'
                         OR description ILIKE 'readycap lending%'
                         OR description ILIKE '%mbe capital%ppp%'
                         OR description ILIKE '%ppp%fund%'
                         OR description ILIKE '%harvest%ppp%'
                         OR description ILIKE '%legacy%bank%ppp%'
                         OR description ILIKE '%ben%cap%ppp%'
                         OR description ILIKE '%tlc pay%'

                         -- REMOVE MORTGAGE LOANS BUT NOT EMPLOYEE SALARY
                         OR (description ILIKE '%MORTG%' AND NOT (description ILIKE '%PAYR%' OR description ILIKE '%DIR%DEP%'))

                         OR description ILIKE '%verge%stride%'
                         OR description ILIKE '%grain technology%'
                         OR description ILIKE '%money network%'

                         -- PROVIDE ADVANCES BASED ON SETTLEMENTS
                         OR description ILIKE '%contractor mgmt%'
                         OR description ILIKE '%contractor manag%'

                         OR description ILIKE '%skrill%'
                         OR description ILIKE '%emoneyusa%'
                         OR description ILIKE '%cardtobank%'
                         OR description ILIKE '%jpmorgan chase%trnsf%'
                         OR description ILIKE '%google%wallet%'
                         OR description ILIKE '%keybank%transf%'
                         OR description ILIKE '%stripe%transfer%'
                         OR description ILIKE '%xoom%transfer%'
                         OR description ILIKE '%optum bank%'

                         -- Transfer from Brokerage / 401k
                         OR description ILIKE '%FID BKG SVC LLC, MONEYLINE%'

                         -- REMOVING ONE-TIME CROWDFUNDERS. LEAVING IN UPTOGETHER B/C SUSPECT HIGHER RECURRENCE
                         OR description ILIKE '%gofundme%'
                         OR description ILIKE '%move%fund%'
                         OR description ILIKE '%stripe%fundr%'

                         -- BELOW LIQUIDATE GIFT CARDS INTO CASH
                         -- DOESN'T HAVE STABILITY IMPLICATIONS OF DIRECT DEPOSITS
                         OR description ILIKE '%giftrocket%'
                         OR description ILIKE '%giftcash%'
                         OR description ILIKE '%evolvebank-p2c%'
                         OR description ILIKE '%RaiseMarketplace%'
                         OR description ILIKE '%CardCash Exchang%'

                         -- TORN ABOUT BELOW
                         -- wondering if should leave b/c they might be paid for services rendered by others?
                         -- REMOVING FOR NOW
                         OR description ILIKE '%zelle%'
                         OR description ILIKE '%p2p%'
                         OR description ILIKE '%venmo%'
                         OR description ILIKE '%debit%'
                         OR description ILIKE '%varo%'
                         OR description ILIKE '%wells fargo%dda to dda%'
                         OR description ILIKE '%wells fargo%plan pmt%'
                         OR description ILIKE '%BANK%AM%'
                         OR description ILIKE '%AMERICAN NAT%'
                         OR description ILIKE '%CIC OF IN%'
                         OR description ILIKE '%TRF%'
                         OR description ILIKE '%deposit evolve%'
                         OR description ILIKE '%APPLE%CASH%TRANSFER%'
                         OR description ILIKE '%VINTED%TRANSFER%'
                         OR description ILIKE '%USAA%TRANSFER%'
                         OR description ILIKE '%HERITAGE%CREDIT%TRANSFER%'
                         OR description ILIKE '%SELFLANE%TRANSFER%'
                         OR description ILIKE '%INSPIRUS%TRANSFER%'
                         OR description ILIKE '%A2A%TRANSFER%'
                         OR description ILIKE '%SMBS%TRANSFER%'
                         OR description ILIKE '%BANNERBANK%TRANSFER%'
                         OR description ILIKE '%BMOHB%TRANSFER%'
                         OR description ILIKE '%AMERICAN%EXPRESS%TRANSFER%'

                         -- REMOVING
                         -- ALL CASHOUTS / DIVIDENDS PAID FROM INVESTMENT AND CRYPTO PLATFORMS
                         -- Did not remove fidelity investments b/c it could be retirement-related funds as well
                         OR description ILIKE '%acorns later%'
                         OR description ILIKE '%acorns invest%'
                         OR description ILIKE '%webull financial%'
                         OR description ILIKE '%coinbase%'
                         OR description ILIKE '%ibotta%'

                         -- WEIRD THAT FUNDS ARE BEING TRANSFERRED FROM THESE TEEN BANKING PLATFORM INTO DAVE
                         OR description ILIKE '%step%transfer%'

                         OR description ILIKE '%uphold%hq%'
                         OR description ILIKE '%coin out%'
                         OR description ILIKE '%apex clearing%'
                         OR description ILIKE '%driveweal%'
                         OR description ILIKE '%robinhood%'
                         OR description ILIKE '%E*TRADE%'
                         OR description ILIKE '%forisus%'
                         OR description ILIKE '%firsttrade secu%'
                         OR description ILIKE '%airwallex%'
                         OR description ILIKE '%futu inc%'
                         OR description ILIKE 'meemo%'
                         OR description ILIKE '%betterment%'
                         OR description ILIKE '%tmx finance%'
                         OR description ILIKE '%FOREX%'
                         OR description ILIKE '%perpay%transfer%'
                         OR description ILIKE '%truebill%transfer%'
                         OR description ILIKE '%sofi money%transfer%'
                         OR description ILIKE '%prizepool%transfer%'

                         -- LOOKS LIKE ITS A REFUND ON PRODUCTS BOUGHT VIA INSTALLMENT PAYMENTS
                         OR description ILIKE '%Sezzle, Payout%'

                         -- THESE ARE FUNDS MEANT TO BE FOR COLLEGE
                         OR description ILIKE '%u-nest holdings%'
                         OR description ILIKE '%dough llc%'

                         -- REMOVING REFUNDS (PRIMARILY UNIVERSITY REFUNDS BUT THERE ARE OTHER ONE-OFFS)
                         OR description ILIKE '%CTU REFUND, REFUND%'
                         OR description ILIKE '%AIU REFUND%'
                         OR description ILIKE '%ECSI%REFUND%'
                         OR description ILIKE '%UNIV%REF%'
                         OR description ILIKE '%NBS%REF%'
                         OR description ILIKE '%RASC%REF%'
                         OR description ILIKE '%KCTC%REF%'
                         OR description ILIKE '%SUEM%REF%'
                         OR description ILIKE '%SNHU%REF%'
                         OR description ILIKE 'LIBERTY%REF%'
                         OR description ILIKE 'HOUCC%REF%'
                         OR description ILIKE 'WAKE%REF%'
                         OR description ILIKE 'MMTC%REF%'
                         OR description ILIKE 'SFASC%REF%'
                         OR description ILIKE 'MMC%REF%'
                         OR description ILIKE 'SUNY%REF%'
                         OR description ILIKE 'ITCC%REF%'
                         OR description ILIKE 'SINCLAIR%REFUND%'
                         OR description ILIKE 'HFC%REFUND%'
                         OR description ILIKE 'TCCD%REFUND%'
                         OR description ILIKE 'COLUMBIA%COLLEGE%REFUND%'
                         OR description ILIKE 'KSU%REFUND%'
                         OR description ILIKE 'LAM%REFUND%'

                         -- AIRLINE
                         OR description ILIKE 'DIXIE%REFUND%'

                         OR description ILIKE 'GSU%REFUND%'
                         OR description ILIKE 'SE%COMM%COLL%REFUND%'

                         -- MIGHT BE VAPE COMPANY?
                         OR description ILIKE 'BATON%REFUND%'

                         OR description ILIKE 'CCSPO%REFUND%'
                         OR description ILIKE 'FSCJ%REFUND%'

                         -- UNITED HEALTH CARE, MOST LIKELY B/C OF OVERPAYMENT
                         OR description ILIKE 'UHC%REFUND%'

                         OR description ILIKE 'ATC%REFUND%'
                         OR description ILIKE 'CAMDEN%COUNTY%REFUND%'
                         OR description ILIKE 'NCU%REFUND%'
                         OR description ILIKE 'STLEO%REFUND%'
                         OR description ILIKE 'TWU%REFUND%'
                         OR description ILIKE 'WILLIAM%PATERSON%REFUND%'
                         OR description ILIKE 'CGTC%REFUND%'
                         OR description ILIKE 'BOSSIER%REFUND%'

                         -- MOST LIKELY METROPCS REFUND?
                         OR description ILIKE 'METRO%REFUND%'

                         OR description ILIKE 'FVTC%REFUND%'
                         OR description ILIKE 'LEC%REFUND%'
                         OR description ILIKE 'TRIDENT%REFUND%'

                         -- BELIEVE IT MGHT BE JOHNSON AND WALES
                         OR description ILIKE 'JWALES%REFUND%'

                         OR description ILIKE 'TRIDENT%REFUND%'
                         OR description ILIKE 'BUTTE%REFUND%'
                         OR description ILIKE 'MSJC%REFUND%'
                         OR description ILIKE 'NGTC%REFUND%'
                         OR description ILIKE 'NEWTC%REFUND%'
                         OR description ILIKE 'UNC%REFUND%'
                         OR description ILIKE 'ASMC%REFUND%'
                         OR description ILIKE 'LACC%REFUND%'
                         OR description ILIKE 'OTCC%REFUND%'
                         OR description ILIKE 'SUS%REFUND%'
                         OR description ILIKE 'UHD%REFUND%'
                         OR description ILIKE 'SANJO%REFUND%'
                         OR description ILIKE 'WSSU%REFUND%'
                         OR description ILIKE 'ARCAD%REFUND%'
                         OR description ILIKE 'KERN%REFUND%'
                         OR description ILIKE 'METRO%COM%REFUND%'
                         OR description ILIKE 'LRCC%REFUND%'
                         OR description ILIKE 'COLORADO STATE%REFUND%'
                         OR description ILIKE 'MDCC%REFUND%'
                         OR description ILIKE 'YCCD%REFUND%'
                         OR description ILIKE 'WGTC%REFUND%'
                         OR description ILIKE 'MSU%REFUND%'
                         OR description ILIKE 'SWTJC%REFUND%'

                         -- CASINO CASHOUTS / LOTTERY
                         OR description ILIKE '%PARX ONLINE CAS%'

                         -- REMOVE CASINO PAYOUTS BUT NOT EMPLOYEE SALARY
                         OR (description ILIKE '%CASINO%' AND NOT (description ILIKE '%PAYR%' OR description ILIKE '%DIR%DEP%'))

                         -- REMOVE MORTGAGE PAYOUTS BUT NOT EMPLOYEE SALARY
                         OR (description ILIKE '%LOTTERY%' AND NOT (description ILIKE '%PAYR%' OR description ILIKE '%DIR%DEP%'))

                         OR description ILIKE '%jackpocket%'
                         OR description ILIKE '%WYNN%MI%'

                         -- SOME SELF LOAN PAYOUTS DON'T ACTUALLY GIVE FUNDS TO CUSTOMERS UNTIL THEY COMPLETE ALL INSTALLMENT PAYMENTS
                         -- IN ORDER FOR CUSTOMERS TO BUILD CREDIT. THIS MEANS THAT UPON RECEIVING PAYOUT, CUSTOMERS ACTUALLY DON'T OWE ANYTHING. SELF.INC
                         -- FOLLOWS THIS MODEL ALTHOUGH I'M UNSURE IF LEAD BANK SELF LEND DOES. NEVERTHELESS THIS CAN STILL BE SEEN AS MONEY MOVEMENT
                         -- AND NOT WAGES EARNED / SALARY SO REMOVING
                         OR description ILIKE '%self lender%'
                         OR description ILIKE '%leadbankselflend%'

                         -- REMOVING ALL GOVERNMENT BENEFITS FROM THIS PORTION OF THE CODE
                         OR description ILIKE '%DUA'
                         OR description ILIKE '%DUA %'
                         OR description ILIKE '%MA DUA%'
                         OR description ILIKE '%PUA%'
                         OR description ILIKE '%UI-%'
                         OR description ILIKE '%UI %'
                         OR description ILIKE '%UI,%'
                         OR description ILIKE '%UI'
                         OR description ILIKE '%BEN%UI'
                         OR description ILIKE '%UI%BEN'
                         OR description ILIKE '%UNEM%'
                         OR description ILIKE '%UEMPLOY%'
                         OR description ILIKE '%STATE%BENEF%'
                         OR description ILIKE '%DOL %'
                         OR description ILIKE '%DOL'
                         OR description ILIKE '%DOL,%'
                         OR description ILIKE '%COMM%PA%BENE%'
                         OR description ILIKE '%MODES,%'
                         OR description ILIKE '%IHSS%'
                         OR description ILIKE '%OTDA%'
                         OR description ILIKE '%ADWS%'
                         OR description ILIKE '%VEC%BENE%'
                         OR description ILIKE '%EMP%LWA%'
                         OR description ILIKE '%DEPT%LAB%LWA%'
                         OR description ILIKE '%NM DWS%'
                         OR description ILIKE '%CARES,%'
                         OR description ILIKE '%DELABOR%CARES%'
                         OR description ILIKE '%OREGON%CARES%'
                         OR description ILIKE '%FEMA%'
                         OR description ILIKE '%DCSE%'
                         OR description ILIKE '%DC%SYS%DIST%'
                         OR description ILIKE '%ODJFS%'
                         OR description ILIKE 'OPM1%310%'
                         OR description ILIKE '%310%TSP%'
                         OR description ILIKE 'MCTF%310%'
                         OR description ILIKE 'VAED%310%'
                         OR description ILIKE 'MCTF%310%'
                         OR description ILIKE 'VAIN%310%'
                         OR description ILIKE 'SSA%310%XXSOC%'
                         OR description ILIKE '%310%MISC%PAY%'
                         OR description ILIKE 'SSI%310%'
                         OR description ILIKE 'VACP%310%'
                         OR description ILIKE '%310%PENSION%'
                         OR description ILIKE '%IDES%PAYMENTS%'
                         OR description ILIKE '%NEVADA%ESD%'

                         -- SEEMS TO BE RETIREE RELATED -- https://www.navyfederal.org/checking-savings/checking/resources/active-duty-posting.html
                         OR description ILIKE '%DFAS%ALT'

                         -- SPECIAL COMPENSATION FOR THOSE INJURED IN COMBAT
                         OR description ILIKE '%DFAS%CRSC%'

                         -- SEEMS TO BE RETIRED ANNUITY
                         OR description ILIKE '%DFAS%AR%ANN%'

                         -- SEEMS TO BE RETIRED ANNUITY
                         OR description ILIKE '%DFAS%SPCLS%'

                         -- RETIRED BENEFITS
                         OR description ILIKE '%DFAS%RET%NET%'

                         OR description ILIKE '%STATE%WISC%SSI'
                         OR description ILIKE '%TREHREMPL%'
                         OR description ILIKE '%NHUC BEN%'
                         OR description ILIKE '%EMPLOYMENT PLUS%'
                         OR description ILIKE '%BENEFIT PAYMENTS%'
                         OR description = 'WIS.DCF/W-2, W2 PAYMENT'
                         OR description = '3801000000000000, FED PAYMNT'
                         OR description ILIKE '%LA REV%COVID%'
                         OR description ILIKE '%vermont det%'

                         -- Economic impact payment
                         OR description ILIKE '%TAX%EIP%'

                         OR description ILIKE '%NEVADA ESD%'
                         OR description ILIKE '%NEB%WORKFORCE%UIPAY%'
                         OR description ILIKE '%HEALTH%HUMAN%INV-PAY%'
                         OR description ILIKE '%HEALTH%HUMAN%SUPPL%'
                         OR description ILIKE '%STATE TREASURER%'
                         OR description ILIKE '%310%FED%TVL%'
                         OR description ILIKE '%310%XXRR%UISI%'
                         OR description ILIKE '%G4S%CARES%'
                         OR description ILIKE '%PAID%LEAVE%BENE%'
                         OR description ILIKE '%PUB%EMP%RET%BEN%'
                        )
                THEN 1

            -- SPECIFIC LOGIC CARVED OUT FOR PMMX
		-- ADD PMVH Q42022 as some PMMX now classified as PMVH
            WHEN funding_flag = 1
                AND external_code in ('PMMX','PMVH')
                AND NOT (
			description='Visa Money Transfer' -- due to inproper configuration, we were not getting the right description so excluding them all
                         OR description ILIKE 'DAVE INC%'
                         OR description ILIKE 'COINBASE%'
                         OR description ILIKE 'MONEYLION%'
                         OR description ILIKE 'ALBERT INSTANT%'
                         OR description ILIKE 'FLOATME%'
                         OR description ILIKE '%CLEO%'
                         OR description ILIKE 'COMMERCE BANK%'
                         OR description ILIKE 'BRIGIT%'
                         OR description ILIKE 'KLOVER%'
                         OR description ILIKE 'WORLD FINANCE%'
                         OR description ILIKE 'MONEYTREE%'
                         OR description ILIKE 'EMPOWER FINANCE%'
                         OR description ILIKE 'ONEMAINLN%'
                         OR description ILIKE 'GAGAN SEKHO%'
                         OR description ILIKE 'DOLLAR FINANCIAL GROUP%'
                         OR description ILIKE 'CHIME%'
                         OR description ILIKE '%TRUEBILL%'
                         OR description ILIKE 'AZLO AZLO%'
                         OR description ILIKE 'AFRIEX%'
                         OR description ILIKE 'LENDNATION%'
                         OR description ILIKE '%LOAN%'
                         OR description ILIKE 'SKY TRAIL%'
                         OR description ILIKE 'EZ MONEY%'
                         OR description ILIKE 'WORLD ACCEPTANCE%'
                         OR description ILIKE '%FINANCE%'
                         OR description ILIKE '%ADVANCE%'
                         OR description ILIKE 'RED RIVER%'
                         OR (description ILIKE '%- Debit' AND NOT description ILIKE '%square%')
                         OR description ILIKE 'SHIRES TERESA%'
                         OR description ILIKE 'GIVEBUTTER%'
                         OR description ILIKE 'CASH FACTORY USA%'
                         OR description ILIKE 'BOONE KHILAH%'
                         OR description ILIKE 'NEIL AW%'
                         OR description ILIKE 'NEAL AW%'
                         OR description ILIKE 'MCCALL MARK%'
                         OR description ILIKE 'CATERINICCHIA MATT%'
                         OR description ILIKE 'MI STAR ONE CREDIT%'

                         -- ANALYZED RECURRENCE OF THESE CHECK LOADS VIA THE INGO MONEY APP AND DECIDED TO REMOVE
                         OR description ILIKE 'FCB INGO%'
                        )
                AND ((unique_users_by_desc_external_code_funding >= 5)
                     OR (unique_users_by_desc_external_code_funding < 5
                         AND (description ILIKE '% LLC%'
                              OR description ILIKE '% INC%'
                              OR description ILIKE '%INSURANCE%'
                              OR description ILIKE 'WESTERN NRG%'
                              OR description ILIKE 'AVAIL PRODUCTIVITY%'
                              OR description ILIKE '%HOUSEKEEPING%'
                              OR description ILIKE 'CABALLERO ENAMORADO%'
                              OR description ILIKE '%PEDIATRICS%'
                              OR description ILIKE 'ALLIANZ GLOBAL%'
                              OR description ILIKE 'PARKWOOD GARDENS%'
                              OR description ILIKE 'STATE FARM%'
                              OR description ILIKE 'THE POWER OF THREE%'
                              OR description ILIKE 'TRAVELERS%'
                             )
                        )
                    )
                THEN 1
            ELSE 0
        END                                                                AS is_non_gov_benefit_dd_candidate

    FROM settled_transactions
    )

SELECT * FROM final