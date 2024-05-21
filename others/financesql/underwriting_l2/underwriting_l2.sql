ALTER SESSION set week_start = 7;

WITH taken_user AS (
   SELECT d.disbursement_ds_pst,
          d.disbursement_method,
          d.PAYBACK_CYCLE,
          d.has_taken,
          d.DISBURSEMENT_AMOUNT,
          d.tip,
          d.has_tip,
          d.fee,
          d.PLEDGED_REVENUE,
          u.REQUESTED_DS_PST,
          u.static_node,
          u.MAX_APPROVED_AMOUNT,
          u.APPROVAL_TYPE,
          u.PREV_ADVANCE_BUCKET,
          u.IS_NEW_USER AS IS_FTCA,
          u.MARKET_ATTRIBUTION,
          u.NEW_MEMBER_FLAG,
          u.UW_PRE_APPROVAL_FLAG
   FROM ANALYTIC_DB.DBT_marts.fct_underwriting_disbursement_revenue AS d 
   LEFT JOIN ANALYTIC_DB.DBT_marts.fct_advance_approval_users AS u
   ON d.APPROVAL_ID = u.advance_approval_id
)

SELECT 
   requested_ds_pst,
   disbursement_ds_pst,
   disbursement_method,
   payback_cycle,
   static_node,
   IS_FTCA,
   max_approved_amount,
   approval_type,
   market_attribution,
   prev_advance_bucket,
   UW_PRE_APPROVAL_FLAG,
   SUM(has_taken) AS TOTAL_TAKEN_COUNT,
   SUM(disbursement_amount) AS TOTAL_DISBURSEMENT_AMOUNT,
   SUM(IFF(IS_FTCA, disbursement_amount, 0)) AS TOTAL_FTCA_DISBURSEMENT_AMOUNT,
   SUM(tip) AS TOTAL_TIP_AMOUNT,
   SUM(has_tip) AS TOTAL_TIP_COUNT,
   SUM(fee) AS TOTAL_FEE_AMOUNT,
   SUM(pledged_revenue) AS TOTAL_PLEDGED_REVENUE
FROM taken_user
WHERE TO_DATE(disbursement_ds_pst) >= DATEADD(WEEK, '-{{look_back_weeks}}', '{{ end_date }}') 
AND TO_DATE(disbursement_ds_pst) < '{{ end_date }}'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY 1,2,3,4,5,6,7,8,9,10,11