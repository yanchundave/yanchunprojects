with sd AS ( --this CTE actually produces no result at the moment
  SELECT
    syn.user_id,
    syn.address_subdivision, syn.id,
    row_number() over (partition BY user_id ORDER BY id DESC) rnk
  FROM APPLICATION_DB.TRANSACTIONS_DAVE.SYNAPSEPAY_DOCUMENT syn
  WHERE address_subdivision IS NOT NULL
  AND permission = 'SEND-AND-RECEIVE'
  AND _FIVETRAN_DELETED='false'
),

--this CTE is getting the state information for Dave users
q0 AS (
  SELECT
    u.id,
    IFF(u.state is NULL OR u.state = '', 'Not Available', u.state) AS "BACKUP_STATE",
    IFF(sd.address_subdivision is NULL OR sd.address_subdivision = '', 'Not Available', sd.address_subdivision) AS "STATE"
  FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER AS u
    LEFT JOIN sd
    ON (u.id = sd.user_id) AND sd.rnk = 1
),

--organize the state info into one column for each Dave user
state_info AS (
	SELECT
	  id AS user_id,
    IFF(STATE = 'Not Available' AND BACKUP_STATE != 'Not Available', BACKUP_STATE, STATE) AS user_state
  FROM q0
),

--gettng payment info for ec up to the report date
ec_payment AS(
  SELECT
    overdraft_id AS advance_id,
    SUM(amount) AS total_settlement
  FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT
  WHERE SETTLEMENT.SETTLEMENT_STATUS_ID = 2 -- 2 means completed
      AND TO_DATE(created) <= '{{report_date}}' -- this is needed to ensure we are only counting payment done on and before the report date!
  GROUP BY advance_id
),

-- getting EC original payback date
sett_date_change AS (
  SELECT o2mod.entity_id AS OVERDRAFT_ID, MODIFICATION
  FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.DASHBOARD_ENTITY_MODIFICATION  o2mod
      JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.DASHBOARD_ACTION_LOG dalog
          ON (o2mod.DASHBOARD_ACTION_LOG_ID = dalog.ID)
      JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.DASHBOARD_ACTION_REASON dareason
          ON (dalog.DASHBOARD_ACTION_REASON_ID = dareason.ID)
      JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.DASHBOARD_ACTION dact
          ON (dareason.DASHBOARD_ACTION_ID = dact.ID)
  WHERE dact.CODE = 'overdraft-settlement-date-change'
),

sett_with_modification AS (
  SELECT
      od.id,
      od.settlement_date,
      sdc.modification
  FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.OVERDRAFT od
      LEFT JOIN sett_date_change sdc
          ON od.id = sdc.overdraft_id
  WHERE TO_DATE(od.created) <= '{{report_date}}'
),

ec_original_payback AS (
  SELECT
      id AS overdraft_id,
      MIN(IFNULL(GET_PATH(elements.value, 'previousValue'), settlement_date)) AS original_payback_date
  FROM sett_with_modification, LATERAL FLATTEN(input => MODIFICATION, OUTER => TRUE) elements
  GROUP BY 1
),

ec_final AS ( -- join with state_info, and filter on 1. only not fully paid advances, and 2. advances created in the last 30 days
  SELECT
    coll.overdraft_id AS advance_id,
    coll.user_id,
    state.user_state,
    coll.created_date,
    coll.due_date AS latest_payback_date,
    op.original_payback_date AS original_payback_date,
    DATEDIFF('day', original_payback_date, latest_payback_date) AS payback_date_diff,
    coll.disbursed_amount AS advance_principal,
    coll.tip_amount,
    (coll.express_fee + coll.service_fee_amount) AS fee,
    coll.total_due,
    ec_payment.total_settlement,
    datediff(day, '{{report_date}}', original_payback_date) AS DPD,
    (coll.total_due - IFNULL(ec_payment.total_settlement, 0)) AS remaining_balance,
    IFF(remaining_balance <= (coll.tip_amount + coll.express_fee + coll.service_fee_amount),
      remaining_balance,
      (coll.tip_amount + coll.express_fee + coll.service_fee_amount)) AS tip_receivables,
    datediff(day, original_payback_date, coll.created_date) AS diff_advance_payback_date
  FROM ANALYTIC_DB.DBT_marts.fct_ExtraCash_Collection coll
    LEFT JOIN state_info state
      ON coll.user_id = state.user_id
    LEFT JOIN ec_payment
      ON coll.overdraft_id = ec_payment.advance_id
    LEFT JOIN ec_original_payback op
      ON coll.overdraft_id = op.overdraft_id
  WHERE
    remaining_balance > 0
      AND created_date >= DATEADD('day', -30, '{{report_date}}')
      AND created_date <= '{{report_date}}'
),

--gettng legacy advance payment info up to the report date
adv_payment AS(
  SELECT
    advance_id,
    SUM(amount) as total_settlement
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.PAYMENT --APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.PAYMENT
    WHERE status = 'COMPLETED'
      AND TO_DATE(created) <= '{{report_date}}' -- this is needed to ensure we are only counting payment done on and before the report date!
    GROUP BY advance_id
),

adv_original_payback AS (
 SELECT
    id AS advance_id,
    MIN(IFNULL(TO_DATE(GET_PATH(elements.value, 'previous.paybackDate')), payback_date)) AS original_payback_date
  FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.ADVANCE, LATERAL FLATTEN(input => modifications, OUTER => TRUE) elements
  WHERE disbursement_status = 'COMPLETED'
    AND TO_DATE(created) <= '{{report_date}}'
  GROUP BY 1
),

adv_final AS ( -- join with state_info, and filter on 1. only not fully paid advances, and 2. advances created in the last 30 days
  SELECT
    coll.advance_id::VARCHAR AS advance_id,
    coll.user_id,
    state.user_state,
    coll.created_date,
    coll.due_date AS latest_payback_date,
    op.original_payback_date AS original_payback_date,
    DATEDIFF('day', original_payback_date, latest_payback_date) AS payback_date_diff,
    coll.disbursed_amount AS advance_principal,
    coll.tip_amount,
    coll.fee,
    coll.total_due,
    adv_payment.total_settlement,
    datediff(day, '{{report_date}}', original_payback_date) AS DPD,
    (coll.total_due - IFNULL(adv_payment.total_settlement, 0)) AS remaining_balance,
    IFF(remaining_balance <= (coll.tip_amount + coll.fee), remaining_balance, (coll.tip_amount + coll.fee)) AS tip_receivables,
    datediff(day, original_payback_date, coll.created_date) AS diff_advance_payback_date
  FROM ANALYTIC_DB.DBT_marts.fct_Advances_Collection coll
    LEFT JOIN state_info state
      ON coll.user_id = state.user_id
    LEFT JOIN adv_payment
      ON coll.advance_id = adv_payment.advance_id
    LEFT JOIN adv_original_payback op
      ON coll.advance_id = op.advance_id
  WHERE
    remaining_balance > 0
      AND created_date >= DATEADD('day', -30, '{{report_date}}')
      AND created_date <= '{{report_date}}'
),

all_txn AS (
  SELECT * FROM adv_final
  UNION ALL
  SELECT * FROM ec_final
),

multi_advancer AS ( -- to identify users who have more than 1 advances outstanding
  SELECT
    user_id,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created_date) AS rank
  FROM all_txn
  QUALIFY rank = 2 -- as long as rank is 2, this users certainly have more than 1 advances outstanding
),

final AS (
  SELECT
    all_txn.*,
    IFF(multi_advancer.user_id IS NOT NULL, 1, 0) AS multi_advance_outstanding,
    IFF(payback_date_diff != 0, 1, 0) AS payback_date_modified
  FROM all_txn
    LEFT JOIN multi_advancer
        ON all_txn.user_id = multi_advancer.user_id
)

SELECT
  TO_VARCHAR(TO_DATE('{{report_date}}','YYYY-mm-dd')) AS "Report Date",
  IFF(advance_principal >= 25, 'Big Money', 'Tiny Money') AS "advance_type",
  SUM(remaining_balance) AS "receivable_outstanding",
  SUM(tip_receivables) AS "tip_fee_receivable",

  SUM(IFF(DPD <= -7, remaining_balance - tip_receivables, 0)) AS "Gross Advances - 7 Days or More Delinquent-Past-Due-Date",
  SUM(IFF(DPD > -7, remaining_balance - tip_receivables, 0)) AS "Gross Advances - 6 Days or Less Delinquent-Past-Due-Date",

  SUM(IFF(advance_principal > 500 AND DPD > -7, remaining_balance - tip_receivables, 0)) AS "Advances in Excess of Size 500",
  SUM(IFF(diff_advance_payback_date < -14 AND DPD > -7 AND advance_principal <= 500, remaining_balance - tip_receivables, 0)) AS "Advances in Excess of Term Criteria",

  -- PA section:
  SUM(IFF(user_state = 'PA' OR user_state = 'pa', remaining_balance - tip_receivables, 0)) AS "pa_receivable_outstanding_minus_tip_fee",
  SUM(IFF(DPD <= -7 AND (user_state = 'PA' OR user_state = 'pa'), remaining_balance - tip_receivables, 0)) AS "pa_users_7_dpd",
  SUM(IFF(advance_principal > 500 AND DPD > -7 AND (user_state = 'PA' OR user_state = 'pa'), remaining_balance - tip_receivables, 0)) AS "pa_users_advances_excess_of_size_500",
  SUM(IFF(diff_advance_payback_date < -14 AND DPD > -7 AND advance_principal <= 500 and (user_state = 'PA' OR user_state = 'pa'), remaining_balance - tip_receivables, 0)) AS "pa_users_terms_criteria",
  SUM(IFF((user_state = 'PA' OR user_state = 'pa') AND diff_advance_payback_date >= -14 AND advance_principal <= 500 AND DPD > -7, remaining_balance - tip_receivables, 0)) AS "pa_advances",
  --WM: pa_advances is essentially pa_receivable_outstanding_minus_tip_fee - pa_users_7_dpd - pa_users_advances_excess_of_size_500 - pa_users_terms_criteria

  -- Additional Ineligible Receivables (modified payback date and/or multi-advance takers)
  SUM(IFF((multi_advance_outstanding = 1 OR payback_date_modified = 1)
      AND user_state NOT IN ('PA', 'pa')
      AND diff_advance_payback_date >= -14
      AND advance_principal <= 500
      AND DPD > -7, remaining_balance - tip_receivables, 0)) AS "additional_ineligible_advances",

  SUM(IFF(advance_principal > 100 AND diff_advance_payback_date >= -14 AND DPD > -7, remaining_balance - tip_receivables, 0)) AS "Eligible Receivables with Advances > 100", -- this probably should exclude PA users?
  --SUM(IFF(DPD > -7 AND DPD < 0 AND advance_principal<=500 AND diff_advance_payback_date >= -14 AND (user_state != 'PA' AND user_state != 'pa'), remaining_balance - tip_receivables, 0)) AS "1-6 DPD - Excess Concentration Limits",
  --SUM(IFF(DPD >= 0 AND advance_principal <= 500 AND diff_advance_payback_date >= -14 AND (user_state != 'PA' AND user_state != 'pa'), remaining_balance - tip_receivables, 0)) AS "Current - Excess Concentration Limits"
  SUM(IFF(DPD > -7 AND DPD < 0 AND advance_principal <= 500 AND diff_advance_payback_date >= -14 AND user_state NOT IN ('PA', 'pa') AND multi_advance_outstanding != 1 AND payback_date_modified != 1,
      remaining_balance - tip_receivables, 0)) AS "1-6 DPD - Excess Concentration Limits",
  SUM(IFF(DPD >= 0 AND advance_principal <= 500 AND diff_advance_payback_date >= -14 AND user_state NOT IN ('PA', 'pa') AND multi_advance_outstanding != 1 AND payback_date_modified != 1,
      remaining_balance - tip_receivables, 0)) AS "Current - Excess Concentration Limits"
FROM final
GROUP BY "advance_type"
ORDER BY "advance_type" ASC -- WM: this is for the Python Notebook to pick up the correct type, not done by me though