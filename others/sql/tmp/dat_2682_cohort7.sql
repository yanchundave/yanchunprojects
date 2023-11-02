--create or replace table sandbox.dev_yyang.unattempt_user_notice as
with
mtm as (
    select user_id, max(dave_last_active_date) as last_mtm_date
    from dbt.adv_churn_marts.fct_dave_segment
    where date_of_interest = dateadd('day', -1, current_date())
    group by 1
),
mau as (
    select
    try_to_number(user_id) as user_id,
    max(date(event_time)) as last_login
    from analytic_db.dbt_marts.amplitude_dao
    where EVENT_TYPE IN ('biometric confirmation success', 'passcode validate success', 'passcode signup success')
    and  date(event_time) >= date('2023-01-01')
    group by 1
),
billdate as (
    select
    a.user_id,
    max(a.bill_due_date) as last_bill_due
    from  ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS a
    join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER b
    on a.user_id = b.id
    where a.bill_due_date >= date('2023-01-01') and b.subscription_fee > 0
    group by 1
),
unattempt as
(
    select user_id
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where billing_cycle = '2023-09' and attempt_cnt = 0
)
select
a.user_id,
b.last_bill_due,
round(datediff('day', c.last_mtm_date, current_date())/30) as month_since_mtm,
round(datediff('day', d.last_login, current_date())/30) as month_since_mau,

from unattempt a
left join billdate b
on a.user_id = b.user_id
left join mtm c
on a.user_id = c.user_id
left join mau d
on a.user_id = d.user_id


------------Generate list

with tmp as (
  select *, uniform(1, 7, random()) as randomnumber
from sandbox.dev_yyang.unattempt_user_notice
where month_since_mtm is not null
and month_since_mau is not null
and last_bill_due is not null
and month_since_mtm <= 12
  )
  select user_id, last_bill_due, month_since_mtm, month_since_mau,
  case
  when randomnumber = 1 then '2023-10-19'
  when randomnumber = 2 then '2023-10-20'
  when randomnumber = 3 then '2023-10-21'
  when randomnumber = 4 then '2023-10-26'
  when randomnumber = 5 then '2023-10-27'
  when randomnumber = 6 then '2023-10-28'
  when randomnumber = 7 then '2023-10-31'
  else 'unnormal'
  end as allocated_date,
  date_add('day', -3, fee_change_notice_date) as fee_change_notice_date
  from tmp