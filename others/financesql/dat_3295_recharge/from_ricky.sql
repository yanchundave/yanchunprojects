WITH first_tip AS (
SELECT
    a.OVERDRAFT_ID
    ,SUM(a.AMOUNT) AS initial_tip_amount
FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.tip_adjustment a
INNER JOIN (SELECT * FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement_level WHERE ITH_DISBURSEMENT = 2) b
ON a.OVERDRAFT_ID = b.OVERDRAFT_ID
AND a.CREATED < b.DISBURSEMENT_TS
GROUP BY 1
),

tip AS (
SELECT
    s.overdraft_id,
    IFNULL(f.initial_tip_amount, s.TIP_AMOUNT) AS first_tip_amount,
    s.tip_amount,
    (s.tip_amount - first_tip_amount) AS second_tip_amount
FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement_summary s
LEFT JOIN first_tip as f
ON s.overdraft_id = f.overdraft_id
)

SELECT
  a.APPROVAL_TYPE,
  d.DISBURSEMENT_DS_PST,
  d.DISBURSEMENT_METHOD,
  d.ITH_DISBURSEMENT,
  SUM(
  CASE
    WHEN ITH_DISBURSEMENT = 1 THEN a.INITIAL_APPROVED_AMOUNT
    WHEN ITH_DISBURSEMENT > 1 THEN a.TOTAL_APPROVED_AMOUNT - a.INITIAL_APPROVED_AMOUNT
    ELSE 0
  END
  ) AS TOTAL_APPROVED_AMOUNT,
  COUNT(d.OVERDRAFT_ID) AS TOTAL_DISBURSEMENT_COUNT,
  SUM(d.OVERDRAFT_AMOUNT) AS TOTAL_DISBURSEMENT_AMOUNT,
  SUM(
  CASE
    WHEN ITH_DISBURSEMENT = 1 THEN t.first_tip_amount
    WHEN ITH_DISBURSEMENT = 2 THEN t.second_tip_amount
    ELSE 0
  END
  ) AS TOTAL_TIP_AMOUNT,
  SUM(d.SERVICE_FEE_AMOUNT + d.EXPRESS_FEE_AMOUNT) AS TOTAL_FEE_AMOUNT,
  TOTAL_TIP_AMOUNT + TOTAL_FEE_AMOUNT AS TOTAL_PLEDGED_REVENUE_AMOUNT
FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement_level d
LEFT JOIN ANALYTIC_DB.DBT_marts.fct_advance_approvals AS a
ON d.APPROVAL_ID = a.ADVANCE_APPROVAL_ID
LEFT JOIN tip AS t
ON d.OVERDRAFT_ID = t.OVERDRAFT_ID
WHERE TO_DATE(a.requested_ds_pst) >= DATEADD(WEEK, '-30', '2024-04-01')
AND TO_DATE(a.requested_ds_pst) < '2024-04-01'