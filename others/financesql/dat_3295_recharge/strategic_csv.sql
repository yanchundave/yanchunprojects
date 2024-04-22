WITH first_tip as (
    select
        a.overdraft_id,
        sum(a.amount) as initial_tip_amount
    from overdraft.overdraft_overdraft.tip_adjustment a
    inner join
        (
            select *
            from analytic_db.dbt_marts.fct_overdraft_disbursement_level
            where ith_disbursement = 2) b
    on a.overdraft_id = b.overdraft_id
        and a.created < b.disbursement_ts
    group by 1
),

tip as (
    select
        s.overdraft_id,
        ifnull(f.initial_tip_amount, s.tip_amount) as first_tip_amount,
        s.tip_amount,
        (s.tip_amount - first_tip_amount) as second_tip_amount
    from analytic_db.dbt_marts.fct_overdraft_disbursement_summary s
    left join first_tip as f
        on s.overdraft_id = f.overdraft_id
),

recharge_performance as (
    select
        d.overdraft_id,
        d.ith_disbursement,
        d.overdraft_amount as disbursement_amount,
        case
            when ith_disbursement = 1 then t.first_tip_amount
            when ith_disbursement = 2 then t.second_tip_amount
            else 0
        end as tip_amount,
        d.service_fee_amount + d.express_fee_amount as fee_amount
    from analytic_db.dbt_marts.fct_overdraft_disbursement_level d
    left join analytic_db.dbt_marts.fct_advance_approvals as a
        on d.approval_id = a.advance_approval_id
    left join tip as t
        on d.overdraft_id = t.overdraft_id
    where to_date(a.requested_ds_pst) < date_trunc('month', current_date())
),


cohort_items AS (
    SELECT
        DATE_TRUNC('MONTH', transacting_ds_pst) AS cohort_month,
        user_id
    FROM ANALYTIC_DB.DBT_metrics.new_transacting_users
    WHERE transacting_ds_pst < DATE_TRUNC('MONTH', CURRENT_DATE())
),

credit_record AS (
    SELECT
        dis.product,
        dis.user_id,
        dis.advance_id,
        1 as ith_disbursement,
        DATE_TRUNC('MONTH', dis.disbursement_ds_pst) AS disburse_month,
        cohort.cohort_month,
        MONTHS_BETWEEN(disburse_month, cohort.cohort_month) AS month_number,
        dis.taken_amount AS advance_amount,
        dis.tip_amount AS tip,
        dis.express_fee_amount + dis.service_fee_amount AS fee
    FROM ANALYTIC_DB.DBT_marts.disbursements dis
        LEFT JOIN cohort_items cohort
            ON dis.user_id = cohort.user_id  AND dis.disbursement_ds_pst >= cohort.cohort_month
    WHERE dis.disbursement_ds_pst < DATE_TRUNC('MONTH', CURRENT_DATE())
),

credit_update as (
    select
        cr.product,
        cr.user_id,
        cr.advance_id,
        cr.disburse_month,
        cr.cohort_month,
        cr.month_number,
        case
            when rp.overdraft_id is null then cr.ith_disbursement
            else rp.ith_disbursement
        end as ith_disbursement,  -- if we found recharge transaction, ith_disbursement is updated based on recharge no.
        case
            when rp.overdraft_id is null then cr.advance_amount
            else rp.disbursement_amount -- if we found recharge transaction, the advance amount is based on recharege disbursement
        end as advance_amount,
        case
            when rp.overdraft_id is null then cr.tip
            else rp.tip_amount
        end as tip,
        case
            when rp.overdraft_id is null then cr.fee
            else rp.fee_amount
        end as fee
    from credit_record cr
    left join recharge_performance rp
    on cr.advance_id = rp.overdraft_id
),

credit_group as (
    select
        cohort_month as first_txn_month,
        disburse_month as current_month,
        month_number as month_number,
        ith_disbursement,
        count(distinct user_id) as unique_borrowers,
        count(distinct advance_id) as num_advances_disbursed,
        sum(advance_amount) as principal_disbursed,
        sum(tip) as pledged_tip,
        sum(fee) as pledged_fee
    from credit_update
    group by 1, 2, 3, 4

),

total_users AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT user_id) AS total_credit_users
    FROM credit_record
    GROUP BY 1
)

SELECT
    credit.first_txn_month,
    credit.current_month,
    credit.month_number,
    total_users.total_credit_users,
    credit.ith_disbursement,
    credit.unique_borrowers,
    credit.num_advances_disbursed,
    credit.principal_disbursed,
    credit.pledged_tip,
    credit.pledged_fee
FROM credit_group credit
    LEFT JOIN total_users
        ON credit.first_txn_month = total_users.cohort_month
