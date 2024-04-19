ALTER SESSION set week_start = 7;

SELECT
   e.requested_ds_pst,
   e.IS_FTCA,
   e.MAX_APPROVED_AMOUNT,
   a.APPROVAL_TYPE,
   e.DISBURSEMENT_METHOD,
   d.DISBURSEMENT_DS_PST,
   COUNT(e.advance_approval_id) AS TOTAL_SESSION_COUNT,
   SUM(e.is_approved) AS TOTAL_APPROVED_COUNT,
   SUM(d.has_taken) AS TOTAL_TAKEN_COUNT,
   SUM(d.has_recharged) AS TOTAL_RECHARGE_COUNT,
   SUM(d.disbursement_amount) AS TOTAL_DISBURSEMENT_AMOUNT,
   SUM(d.initial_disbursement_amount) AS TOTAL_INITIAL_DISBURSEMENT_AMOUNT,
   SUM(d.recharge_disbursement_amount) AS TOTAL_RECHARGE_DISBURSEMENT_AMOUNT,
   SUM(d.tip) AS TOTAL_TIP_AMOUNT,
   SUM(d.has_tip) AS TOTAL_TIP_COUNT,
   SUM(d.fee) AS TOTAL_FEE_AMOUNT,
   SUM(d.initial_pledged_revenue) AS TOTAL_INITIAL_PLEDGED_REVENUE,
   SUM(d.pledged_revenue) AS TOTAL_PLEDGED_REVENUE,
   SUM(d.recharge_pledged_revenue) AS TOTAL_RECHARGE_PLEDGED_REVENUE,
   COUNT(distinct case when d.has_taken=1 then d.user_id  else null end ) as unique_taken_user_count,
   COUNT(distinct case when d.has_recharged=1 then d.user_id  else null end ) as unique_recharge_user_count

FROM ANALYTIC_DB.DBT_marts.fct_underwriting_ec_session_requests AS e
LEFT JOIN ANALYTIC_DB.DBT_marts.fct_advance_approvals AS a
ON e.ADVANCE_APPROVAL_ID = a.ADVANCE_APPROVAL_ID
LEFT JOIN ANALYTIC_DB.DBT_marts.fct_underwriting_disbursement_revenue AS d
ON e.ADVANCE_APPROVAL_ID = d.APPROVAL_ID
WHERE TO_DATE(e.requested_ds_pst) >= DATEADD(WEEK, '-30', '2024-04-03')
AND TO_DATE(a.requested_ds_pst) < '2024-04-03'
AND e.ineligible_reason = 'eligible pass'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6