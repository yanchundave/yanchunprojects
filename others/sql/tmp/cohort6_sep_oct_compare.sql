with users as (

    select id as user_id from SANDBOX.DEV_ASCHMIDLI.subs_cohort_6
),
sep as (

    select
        a.user_id,
        b.billing_cycle,
        b.bill_due_date,
        b.is_collect_succeeded,
        b.attempted_to_collect,
        b.attempt_cnt
    from users a
    left join sandbox.dev_yyang.legacy_oct23 b
    on a.user_id = b.user_id
    where billing_cycle = '2023-09'
),
oct as (
    select
        a.user_id,
        b.billing_cycle,
        b.term_started as bill_due_date,
        b.is_collect_succeeded,
        b.attempted_to_collect,
        b.attempt_cnt
    from users a
    left join sandbox.dev_yyang.sub2_oct25 b
    on a.user_id = b.user_id
    where billing_cycle = '2023-10'
)
select * from sep
union all
select * from oct

-----------------------------

with users as (

    select id as user_id from SANDBOX.DEV_ASCHMIDLI.subs_cohort_6
),
oct as (
    select
        a.user_id,
        b.billing_cycle,
        b.term_started as bill_due_date,
        b.is_collect_succeeded,
        b.attempted_to_collect,
        b.attempt_cnt
    from users a
    left join sandbox.dev_yyang.sub2_oct25 b
    on a.user_id = b.user_id
    where billing_cycle = '2023-10'
),
failed_user as (
    select user_id
    from oct
    where is_collect_succeeded = 0 and attempted_to_collect = 1
)
select b.*, a.event_type, a.event_properties:reason
from failed_user b
left join analytic_db.dbt_marts.amplitude_dao a
on try_to_number(a.user_id) = b.user_id
where a.event_type in (
'subscription ach failed',
'subscription ach not allowed',
'subscription ach rescheduled',
'subscription ach returned',
'subscription charge not allowed',
'subscription charged payment method invalid',
'subscription no payment method on file'
) and date(a.event_time) >= Date('2023-10-01')
qualify row_number() over (partition by b.user_id order by a.server_received_time desc) = 1