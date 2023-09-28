create or replace table sandbox.dev_yyang.user_cohort6 as
WITH previousbill as
(
    select user_id, sum(is_collect_succeeded) as totalcollect from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
  where bill_due_date >= date_trunc('month', dateadd('month', -6, current_date()))
  group by user_id
  having sum(is_collect_succeeded) > 0
),
applogin as
(
    select distinct try_to_number(user_id) as user_id from analytic_db.dbt_marts.amplitude_dao
    where EVENT_TYPE IN ('biometric confirmation success', 'passcode validate success', 'passcode signup success')
    and  date(event_time) >= dateadd('month', -6, current_date())
)

select
a.USER_ID,
a.bill_due_date,
CASE
    WHEN BC_HEALTHINESS_SCORE = 1 THEN 'HEALTH'
    WHEN BC_HEALTHINESS_SCORE = 2 THEN 'UNHEALTH'
    WHEN BC_HEALTHINESS_SCORE = 3 THEN 'DISCONNECTED'
    ELSE 'OTHERS'
END AS HEALTH,

case
    when p.user_id is null then 0
    else 1
end as if_spending_within_6months,

case
    when l.user_id is null then 0
    else 1
end as if_login_within_6months
from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS a
left join previousbill p on a.user_id = p.user_id
left join applogin l on a.user_id = l.user_id
left join DAVE.subscription.subscription sub
on a.user_id = sub.user_id
where a.BC_HEALTHINESS_SCORE > 0
AND a.BILLING_CYCLE = '2023-09'
AND sub.user_id is NULL


