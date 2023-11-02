create or replace table sandbox.dev_yyang.dat_2689 as
with tmp as (
  select
    id as subscription_id,
    user_id,
    started,
    ended
  from DAVE.subscription.subscription
  where ended is not null and canceled is null  and date(started) > date('2023-06-09')
),
duedate as
(
    select
        a.subscription_id,
        a.term_started,
        sc_status.code as collect_status
    from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE a
    LEFT JOIN DAVE.SUBSCRIPTION.subscription_charge_status sc_status ON a.subscription_charge_status_id = sc_status.id
    where a.unit_cost > 0
    Qualify row_number() over (partition by a.subscription_id order by a.term_started desc) = 1
)
select
  tmp.user_id as user_id,
  duedate.term_started as latest_due_date,
  date(tmp.started) as started,
  date(tmp.ended) as ended,
  duedate.collect_status
from tmp
left join duedate
on tmp.subscription_id = duedate.subscription_id