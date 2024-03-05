---version 1

with
subfee as (
    select distinct user_id
    from ANALYTIC_DB.DBT_metrics.total_dave_revenue
    where SUBSCRIPTION_REVENUE > 0
    ),
 users_list as (
    select
    distinct user_id
    from ANALYTIC_DB.DBT_metrics.one_dave_transacting_users
    where transacting_ds_pst >= date('2021-08-01') and transacting_ds_pst < date('2022-03-01')
    union
    select
    distinct user_id
    from ANALYTIC_DB.DBT_metrics.one_dave_new_members
    where event_ds >= date('2021-08-01') and event_ds < date('2022-03-01')
    )
    select
    case
    when c.user_id is null then 'notpay'
    else 'pay'
    end as subpay,
    count(distinct a.user_id)
    from users_list a
    left join ANALYTIC_DB.DBT_metrics.one_dave_new_members b
    on a.user_id = b.user_id
    left join subfee c
    on a.user_id = c.user_id
    where b.new_member_category = 'dco'
    group by 1

    ----version 2


    with users_list as (
    select
    distinct user_id
    from ANALYTIC_DB.DBT_metrics.one_dave_transacting_users
    where transacting_ds_pst >= date('2022-11-01') and transacting_ds_pst < date('2023-04-01')

    )
    select
    b.new_member_category,
    count(distinct a.user_id)
    from users_list a
    left join ANALYTIC_DB.DBT_metrics.one_dave_new_members b
    on a.user_id = b.user_id
    group by 1