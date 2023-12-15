--From stan request

with bau_attempt as (
select
    date(scheduled_for) as trigger_time, count(distinct subscription_charge_id) as total_sub
from analytic_db.dbt_marts.fct_subscription2_attempts
where term_started >= date('2023-11-14')
and charge_trigger = 'bau_event'
group by 1
),
bau_success as (
select
    date(scheduled_for) as trigger_time, count(distinct subscription_charge_id) as success_sub
from analytic_db.dbt_marts.fct_subscription2_attempts
where term_started >= date('2023-11-14')
and charge_trigger = 'bau_event' and completed_at is not null and returned_at is null and payment_attempt = 'ach'
group by 1
)
select
    a.trigger_time,
    a.total_sub,
    b.success_sub,
    b.success_sub / a.total_sub as success_rate
from bau_attempt a
join bau_success b
on a.trigger_time = b.trigger_time