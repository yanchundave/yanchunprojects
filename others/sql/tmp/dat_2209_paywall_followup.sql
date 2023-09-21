with ab_assign as (
    SELECT
        user_id,
        event_properties:"subscriptions-paywall-placement.variant" AS test_variant,
        MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time)) AS start_ts_pt
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    WHERE EVENT_TYPE = '[Experiment] Assignment'
        AND event_time >= '2023-05-10'
        AND event_properties:"subscriptions-paywall-placement.variant" in ('control', 'treatment')
        GROUP BY 1,2
),
view_event as (
    select user_id
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2023-05-10')
    and event_type like '%subscriptions paywall screen viewed%'
),
submit_event as (
    select user_id, EVENT_PROPERTIES:"tier" as submit_property
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2023-05-10')
    and event_type like '%subscriptions paywall selection submitted%'
),
final as
(
    select
        ab_assign.user_id,
        ab_assign.test_variant,
        ab_assign.start_ts_pt,
        COALESCE(submit_event.submit_property, 'NONE') as submit_property
    from ab_assign
    left join view_event
    on ab_assign.user_id = view_event.user_id
    left join submit_event
    on ab_assign.user_id = submit_event.user_id
),
users as 
(
    select 
        user_id,
        start_ts_pt,
        submit_property
    from final 
    where submit_property in ('dave-plus', 'dave-lite') and date(start_ts_pt) <= date('2023-05-26')
)
select 
 a.user_id,
 a.start_ts_pt,
 a.submit_property,
 requests.advance_id,
 max(requests.max_approved_amount) as max_amount,
 max(requests.is_approved) as is_approved,
 max(requests.is_disbursed) as is_disbursed
from users  a 
join analytic_db.dbt_marts.requests requests
on a.user_id = requests.user_id
where datediff('day', a.start_ts_pt, requests.requested_ds_pst) <= 30
group by 1, 2, 3, 4