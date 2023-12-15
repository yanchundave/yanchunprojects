with new_mtm as (
    SELECT user_id, min(date_trunc('month', TRANSACTING_TS_PST)) as earliest_mtm_date
    FROM ANALYTIC_DB.DBT_metrics.one_dave_transacting_users GROUP BY 1
),
sub_user as (
    select user_id, billing_cycle from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    union all
    select user_id, date_trunc('month', term_started) as billing_cycle from analytic_db.dbt_marts.fct_subscription2_collections
),
subscription_users as
(
    select
         user_id,
         min(billing_cycle) as earilest_scheduled,
         max(billing_cycle) as latest_scheduled
    from sub_user
    group by 1
)
select
    a.user_id,
    a.earliest_mtm_date,
    case
    when b.user_id is not null then 1
    else 0
    end as if_subscriber,
    b.earilest_scheduled,
    b.latest_scheduled
from new_mtm a
left join subscription_users b
on a.user_id = b.user_id
where a.earliest_mtm_date >= date('2023-10-01')