with ab_ct as (
    SELECT
        user_id,
        event_properties:"subscriptions-paywall-placement.variant" AS test_variant,
        MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time)) AS start_ts_pt
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    WHERE EVENT_TYPE = '[Experiment] Assignment'
        AND event_time >= '2023-05-11'
        AND event_properties:"subscriptions-paywall-placement.variant" in ('control', 'treatment')
        GROUP BY 1,2
),
ab_off as
(
    select
        b.user_id,
        to_variant('control') AS test_variant,
        MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', b.event_time)) AS start_ts_pt
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_ATTRIBUTION a
    join  ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO b
    on a.user_id = b.user_id
    WHERE b.EVENT_TYPE = '[Experiment] Assignment'
        AND b.event_time >= '2023-05-10'
        AND b.event_properties:"subscriptions-paywall-placement.variant" in ('off')
        AND b.user_id is not null and b.user_id <> 'NaN'
        AND DATE(a.PV_TS) >= DATE('2023-05-11')
        GROUP BY 1,2

),
ab_assign as (
    select * from ab_ct
    union
    select * from ab_off
),

view_event as (
    select
        user_id
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2023-05-10')
    and event_type like '%subscriptions paywall screen viewed%'
    group by 1
),
view_failed as (
    select
        user_id
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2023-05-10')
    and event_type like '%subscriptions paywall selection failed%'
    group by 1
),
view_success as
(
   select
        user_id
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2023-05-10')
    and event_type like '%subscriptions paywall selection success%'
    group by 1
),
advance_taken as (
    SELECT
        disbursement.user_id,
        disbursement.OVERDRAFT_ID,
        disbursement.disbursement_ts_pst
    FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement disbursement
    JOIN ab_assign
    ON disbursement.user_id = ab_assign.user_id
      AND DATE(disbursement.disbursement_ts_pst) <= DATE(ab_assign.start_ts_pt) + 6
    WHERE disbursement_ds_pst >= '2023-05-10' -- rollout date
        AND disbursement_status = 'COMPLETE'

),
user_data as
(
    select
        ab_assign.user_id,
        ab_assign.test_variant,
        DATE(ab_assign.start_ts_pt) AS ab_test_dt_pst,
        MAX(IFF(DATE(advance_taken.disbursement_ts_pst) <= DATE(ab_assign.start_ts_pt) + 6, 1, 0)) AS is_7d_taken
    from ab_assign
    left join advance_taken
    on ab_assign.user_id = advance_taken.user_id
    group by 1, 2, 3
)
SELECT
    a.user_id,
    a.test_variant,
    a.ab_test_dt_pst,
    a.is_7d_taken,
    IFF(b.user_id is null, 0, 1) AS if_viewed,
    IFF(c.user_id is null, 0, 1) as if_viewed_not_succeeded,
    IFF(d.user_id is null, 0, 1) as if_viewed_succeeded
FROM user_data a
left join view_event b
on a.user_id = b.user_id
left join view_failed c
on a.user_id = c.user_id
left join view_success d
on a.user_id = d.user_id;
