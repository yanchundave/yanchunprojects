with total_attempt as (
with
nov as (
select * from analytic_db.dbt_marts.fct_subscription2_attempts where attempt_dt >= date('2023-11-01') and attempt_dt <= dateadd('month', -2, current_date())
),
dec as (
select * from analytic_db.dbt_marts.fct_subscription2_attempts where attempt_dt >= date('2023-12-01') and attempt_dt <= dateadd('month', -1, current_date())
),
jan as (
select * from analytic_db.dbt_marts.fct_subscription2_attempts where attempt_dt >= date('2024-01-01') and attempt_dt <=  current_date()
),
final as (
select * from nov
union all
select * from dec
union all
select * from jan
)
select date(attempt_dt) as dates, count(distinct subscription_charge_id) as attempted_count from final group by 1 order by 1
),
attempt_success as (
with
nov as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where last_attempt_date >= date('2023-11-01') and last_attempt_date <= dateadd('month', -2, current_date())
),
dec as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where last_attempt_date >= date('2023-12-01') and last_attempt_date <= dateadd('month', -1, current_date())
),
jan as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where last_attempt_date >= date('2024-01-01') and last_attempt_date <= current_date()
),
final as (
select * from nov
union all
select * from dec
union all
select * from jan
)
select date(last_attempt_date) as dates, count(distinct user_id) as attempted_success from final where collect_status = 'collected' group by 1 order by 1
),
term_success as (
with
nov as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where term_started >= date('2023-11-01') and term_started <= dateadd('month', -2, current_date())
),
dec as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where term_started >= date('2023-12-01') and term_started <= dateadd('month', -1, current_date())
),
jan as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where term_started >= date('2024-01-01') and term_started <= current_date()
),
final as (
select * from nov
union all
select * from dec
union all
select * from jan
)
select date(term_started) as dates, count(distinct user_id) as term_success from final where collect_status = 'collected' group by 1 order by 1
),
term_total as (
with
nov as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where term_started >= date('2023-11-01') and term_started <= dateadd('month', -2, current_date())
),
dec as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where term_started >= date('2023-12-01') and term_started <= dateadd('month', -1, current_date())
),
jan as (
select * from analytic_db.dbt_marts.fct_subscription2_collections where term_started >= date('2024-01-01') and term_started <= current_date()
),
final as (
select * from nov
union all
select * from dec
union all
select * from jan
)
select date(term_started) as dates, count(distinct user_id) as term_total from final group by 1 order by 1
)
select
    a.dates,
    dayofmonth(a.dates) as dayofmonth,
    a.attempted_count,
    b.attempted_success,
    c.term_total,
    d.term_success,
    b.attempted_success / a.attempted_count as attempt_success,
    d.term_success / c.term_total as term_success
from total_attempt a
left join attempt_success b
on a.dates = b.dates
left join term_total c
on a.dates = c.dates
left join term_success d
on a.dates = d.dates