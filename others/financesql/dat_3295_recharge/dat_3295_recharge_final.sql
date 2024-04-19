with first_tip as (
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

cohort_items as (
    select
        date_trunc('month', transacting_ds_pst) as cohort_month,
        user_id
    from analytic_db.dbt_metrics.new_transacting_users
    where transacting_ds_pst < date_trunc('month', current_date())
),

settlement as ( -- this is for the outstanding amount past due date
    select
        advance_id,
        iff(outstanding_amount > 0, outstanding_amount, 0) as outstanding_amount -- to take care of over settlement credit txns
    from analytic_db.dbt_marts.settlements
    where settlement_due_ds_pst < current_date()
),

settlement_30d_past_due as ( -- this is for the outstanding amount past 30 days after due date
    select
        advance_id,
        iff(outstanding_amount > 0, outstanding_amount, 0) as outstanding_amount -- to take care of over settlement credit txns
    from analytic_db.dbt_marts.settlements
    where settlement_due_ds_pst < dateadd(day, -30, current_date())
),

credit_record as (
    select
        dis.product,
        dis.user_id,
        dis.advance_id,
        1 as ith_disbursement, -- Temporarily set all the disbursement as the first disbursement
        date_trunc('month', dis.disbursement_ds_pst) as disburse_month,
        cohort.cohort_month,
        months_between(disburse_month, cohort.cohort_month) as month_number,
        dis.taken_amount as advance_amount,
        dis.tip_amount as tip,
        dis.express_fee_amount + dis.service_fee_amount as fee,
        sett.outstanding_amount,
        sett_30d.outstanding_amount as outstanding_amount_30d
    from analytic_db.dbt_marts.disbursements dis
    left join cohort_items cohort
        on dis.user_id = cohort.user_id  and dis.disbursement_ds_pst >= cohort.cohort_month
    left join settlement sett
        on dis.advance_id = sett.advance_id
    left join settlement_30d_past_due sett_30d
        on dis.advance_id = sett_30d.advance_id
    where dis.disbursement_ds_pst < date_trunc('month', current_date())
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
        end as fee,
        case
            when ith_disbursement = 1 then cr.outstanding_amount
            else 0
        end as outstanding_amount, -- only ith_disbursement = 1 then we count outstanding in case multiple counting outstanding amount needn't make difference
        case
            when ith_disbursement = 1 then cr.outstanding_amount_30D
            else 0
        end as outstanding_amount_30D
    from credit_record cr
    left join recharge_performance rp
    on cr.advance_id = rp.overdraft_id
)

select
    credit.product as product_type,
    credit.cohort_month as first_txn_month,
    credit.month_number,
    credit.ith_disbursement,  -- added ith_disbursement
    count(distinct credit.user_id) as unique_borrowers,
    count(distinct credit.advance_id) as num_advances_disbursed,
    sum(credit.advance_amount) as principal_disbursed,
    sum(credit.tip) as pledged_tip,
    sum(credit.fee) as pledged_fee,
    sum(credit.outstanding_amount) as uncollected_receivables,
    sum(credit.outstanding_amount_30d) as uncollected_receivables_30d
from credit_update credit
group by 1, 2, 3, 4
