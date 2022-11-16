DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.collection_payment;
CREATE table DBT.DEV_YANCHUN_PUBLIC.collection_payment
AS
WITH advance_payments AS
(
  SELECT DISTINCT
    CAST(a.advance_id AS varchar) AS overdraft_id,
    a.CHOSEN_ADVANCE_APPROVAL_ID AS approval_id,
    a.disbursement_ds,
    a.payback_ds AS settlement_due_date,
    d.week_start_ds,
    f.event_ds AS payment_ds,
    DATEDIFF(DAY, a.payback_ds, f.event_ds) AS days_since_payback_date,
    a.advance_amount AS disbursement_amount,
    a.tip,
    a.fee,
    a.amount_due,
    ZEROIFNULL(q.paid_0) AS zero_day_accumulative_payment,
    SUM(ZEROIFNULL(p.payment_amount)) AS daily_payment_since_zero_day
  FROM
    analytic_db.dbt_marts.fct_advances a
    INNER JOIN analytic_db.dbt_marts.dim_dates d
        ON a.payback_ds = d.event_ds
    CROSS JOIN
        (SELECT event_ds FROM analytic_db.dbt_marts.dim_dates) f
    LEFT JOIN analytic_db.dbt_marts.fct_payments p
        ON a.advance_id = p.advance_id
            AND DATEDIFF(DAY, a.payback_ds, f.event_ds) = p.days_since_payback_date
    LEFT JOIN ANALYTIC_DB.DBT_MARTS.FCT_ADVANCE_PAYBACK q
        ON a.advance_id = q.advance_id
  WHERE
    a.PAYBACK_DS >= DATEADD(WEEK, '-42', '2022-10-19')
    AND a.PAYBACK_DS < DATEADD(WEEK, 0, '2022-10-19')
    AND f.event_ds BETWEEN a.payback_ds - 0 AND a.payback_ds + 120
  GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12
),
extra_cash_settle AS
(
    SELECT
        OVERDRAFT_ID,
        TO_DATE(SETTLEMENT_DUE_DS) AS payback_date,
        MAX(IFF(SETTLEMENT_DS <= SETTLEMENT_DUE_DS, CUMULATIVE_SETTLEMENT, 0)) AS paid_0
    FROM ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_SETTLEMENT
    GROUP BY 1, 2
),
extra_cash_payments AS (
    SELECT DISTINCT
        o.overdraft_id,
        o.approval_id,
        o.disbursement_ds,
        to_date(o.settlement_due_ds) AS settlement_due_date,
        d.week_start_ds,
        f.event_ds AS payment_ds,
        DATEDIFF(DAY, settlement_due_date, f.event_ds) AS days_since_payback_date,
        o.overdraft_amount AS disbursement_amount,
        o.tip_amount as tip,
        (o.service_fee_amount + o.express_fee_amount) AS fee,
        o.amount_due,
        ZEROIFNULL(q.paid_0) AS zero_day_accumulative_payment,
        SUM(ZEROIFNULL(p.settlement_amount)) AS daily_payment_since_zero_day
    FROM
      analytic_db.dbt_marts.fct_overdraft_disbursement o
      INNER JOIN analytic_db.dbt_marts.dim_dates d
          ON settlement_due_date = d.event_ds
      CROSS JOIN
          (SELECT event_ds FROM analytic_db.dbt_marts.dim_dates) f
      LEFT JOIN analytic_db.dbt_marts.fct_overdraft_settlement p
          ON o.overdraft_id = p.overdraft_id AND p.settlement_ds = f.event_ds
      LEFT JOIN extra_cash_settle q
          ON o.overdraft_id = q.overdraft_id
    WHERE
      settlement_due_date >= DATEADD(WEEK, '-42', '2022-10-19')
      AND settlement_due_date < DATEADD(WEEK, 0, '2022-10-19')
      AND f.event_ds BETWEEN settlement_due_date - 0 AND settlement_due_date + 120
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
all_payments AS (
  SELECT * FROM advance_payments
  UNION
  SELECT * FROM extra_cash_payments
)
  SELECT
    overdraft_id,
    approval_id,
    disbursement_ds,
    settlement_due_date AS settlement_due_ds,
    week_start_ds,
    payment_ds,
    days_since_payback_date,
    disbursement_amount,
    tip,
    fee,
    (tip + fee) AS pledged_revenue,
    (disbursement_amount + tip + fee) AS cal_amount_due,
    amount_due,
    zero_day_accumulative_payment,
    daily_payment_since_zero_day,
    CASE
        WHEN days_since_payback_date = 0 THEN GREATEST(zero_day_accumulative_payment, daily_payment_since_zero_day)
        ELSE daily_payment_since_zero_day
    END AS daily_payment,
    SUM(daily_payment) OVER(
      PARTITION BY overdraft_id
      ORDER BY
        days_since_payback_date ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS cumulative_payment,
    (cumulative_payment - disbursement_amount) AS net_revenue,
    (amount_due - cumulative_payment) AS outstanding_balance
  FROM
    all_payments;