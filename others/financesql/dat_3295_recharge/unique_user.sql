with disburse_level as (
    select
        overdraft_id,
        ith_disbursement
    from analytic_db.dbt_marts.fct_overdraft_disbursement_level

),

cohort_items as (
    select
        date_trunc('month', transacting_ds_pst) as cohort_month,
        user_id
    from analytic_db.dbt_metrics.new_transacting_users
    where transacting_ds_pst < date_trunc('month', current_date())
),

credit_record as (
    select
        dis.user_id,
        date_trunc('month', dis.disbursement_ds_pst) as disburse_month,
        coalesce(dl.ith_disbursement, 1) as ith_disbursement,
        cohort.cohort_month,
        months_between(disburse_month, cohort.cohort_month) as month_number
    from  analytic_db.dbt_marts.fct_overdraft_disbursements dis
    left join cohort_items cohort
        on dis.user_id = cohort.user_id  and dis.disbursement_ds_pst >= cohort.cohort_month
    left join disburse_level dl
        on dis.advance_id = dl.overdraft_id
    where dis.disbursement_ds_pst < date_trunc('month', current_date())
)

select
    credit.cohort_month as first_txn_month,
    credit.month_number,
    credit.ith_disbursement,
    count(distinct credit.user_id) as unique_borrowers
from credit_record credit
group by 1, 2,3