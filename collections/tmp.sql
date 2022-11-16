WITH
all_cumulative_payments as
(
    SELECT * FROM DBT.DEV_YANCHUN_PUBLIC.collection_payment
),
users AS (
  SELECT user_id,
         disbursement_type,
         COALESCE(CAST(advance_id AS varchar), overdraft_id) AS overdraft_id,
         PREV_ADVANCE_BUCKET,
         is_new_user,
         attribution AS customer_source,
         ML_NODE,
         STATIC_NODE,
         MAX_APPROVED_AMOUNT,
         ADVANCE_TAKEN_AMOUNT
    FROM analytic_db.dbt_marts.dim_advance_users
)
weekly_batch AS (
  SELECT a.week_start_ds,
         a.settlement_due_ds,
         a.days_since_payback_date,
         u.is_new_user,
         u.ml_node,
         u.static_node,
         u.max_approved_amount AS approved_amount,
         u.advance_taken_amount AS taken_amount,
         COUNT(a.overdraft_id) AS total_taken_count,
         SUM(a.net_revenue) AS total_net_revenue,
         SUM(a.outstanding_balance) AS outstanding_balance,
         SUM(a.amount_due) AS total_amount_due
  FROM users u
  INNER JOIN all_cumulative_payments a
  USING (overdraft_id)
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER By 1,2,3,4,5,6,7,8
)
select