create or replace table sandbox.dev_yyang.lossrate_unmature as
with tmp as (
    select
    disbursement_ds_pst,
    case
    when original_payback_date >= current_date() then 1
    else 0
    end as unmature
    from ANALYTIC_DB.DBT_marts.disbursements
)
select disbursement_ds_pst, sum(unmature)/count(*) as unmature_rate from tmp group by 1