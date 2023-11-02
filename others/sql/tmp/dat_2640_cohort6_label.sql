--create or replace table sandbox.dev_yyang.cohort6_label as
with
legacy_user as
(
    select * from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
),
past_3month as (
    select
        user_id,
        sum(is_collect_succeeded),
        max(payment_pt_dt) as latest_payment_3month
    from legacy_user
    where billing_cycle in ('2023-07', '2023-08', '2023-09') and is_collect_succeeded = 1
    group by 1
),
past_6month as (
    select
        user_id,
        sum(is_collect_succeeded),
        max(payment_pt_dt) as latest_payment_6month
    from legacy_user
    where billing_cycle in ('2023-04', '2023-05', '2023-06', '2023-07', '2023-08', '2023-09') and is_collect_succeeded = 1
    group by 1
),
sep as
(
    select
        a.user_id,
        b.is_existing_subscriber,
        b.is_collect_succeeded,
        case
        when b.attempt_cnt > 0 then 1
        else 0
        end as is_attempted,
        CASE b.BC_HEALTHINESS_SCORE
            WHEN 1 THEN '1 HEALTHY'
            WHEN 2 THEN '2 UNHEALTHY'
            WHEN 3 THEN '3 DISCONNECTED'
            ELSE '4 NULL'
        END as healthiness
    from cohort6 a
    left join legacy_user b
    on a.user_id = b.user_id
    where b.billing_cycle = '2023-09'
),
combination as (
    select
        sep.*,
        case
            when b.user_id is not NULL then 1
            else 0
        end as is_success_past3month,
        case
            when c.user_id is not null then 1
            else 0
        end as is_success_pass6month,
        b.latest_payment_3month,
        c.latest_payment_6month
    from sep
    left join past_3month b
    on sep.user_id = b.user_id
    left join past_6month c
    on sep.user_id = c.user_id
)
select * from combination


--------------------------Update with bill_date

create or replace table sandbox.dev_yyang.user_billduedate as
with
legacy_user as
(
    select * from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
),
sep as
(
    select user_id, bill_due_date from legacy_user where billing_cycle = '2023-09' and attempt_cnt = 0
),
past_3month as (
    select
        user_id,
        max(bill_due_date) as latest_bill_due_date
    from legacy_user
    where billing_cycle in ('2023-07', '2023-08', '2023-06') and bill_due_date is not null
    group by 1
),
past_6month as (
    select
        user_id,
        max(bill_due_date) as latest_bill_due_date
    from legacy_user
    where billing_cycle in ('2023-04', '2023-05', '2023-06', '2023-07', '2023-08', '2023-03') and bill_due_date is not null
    group by 1
),
combination as (
    select
        sep.user_id,
        sep.bill_due_date as sep_bill_due_date,
        b.latest_bill_due_date as latest_due_date_3month,
        c.latest_bill_due_date as latest_due_date_6month,
        DATE_TRUNC('month', b.latest_bill_due_date) as first_date_3month,
        DATE_TRUNC('month', c.latest_bill_due_date) as first_date_6month,
        case
        when datediff('day', first_date_3month, latest_due_date_3month) >= 16 then 1
        else 0
        end as had_due_over_17_3month,
        case
        when datediff('day', first_date_6month, latest_due_date_6month) >= 16 then 1
        else 0
        end as had_due_over_17_6month,
        case
        when latest_due_date_3month is not null then 1
        else 0
        end as has_due_3month,
        case
        when latest_due_date_6month is not null then 1
        else 0
        end as has_due_6month
    from sep
    left join past_3month b
    on sep.user_id = b.user_id
    left join past_6month c
    on sep.user_id = c.user_id
)
select * from combination