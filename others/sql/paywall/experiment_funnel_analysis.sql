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
view_failed as (
    select user_id
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2023-05-10')
    and event_type like '%subscriptions paywall selection failed%'
),
view_success as
(
   select user_id
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2023-05-10')
    and event_type like '%subscriptions paywall selection success%'
),
final as
(
    select
        ab_assign.user_id,
        ab_assign.test_variant,
        ab_assign.start_ts_pst,
        IFF(view_event.user_id is NULL, 0, 1) as if_view,
        IFF(view_failed.user_id is NULL, 0, 1) as view_failed,
        IFF(view_success.user_id is NULL, 0, 1) as view_success
    from ab_assign
    left join view_event
    on ab_assign.user_id = view_event.user_id
    left join view_failed
    on ab_assign.user_id = view_failed.user_id
    left join view_success
    on ab_assign.user_id = view_success.user_id
)
select * from final