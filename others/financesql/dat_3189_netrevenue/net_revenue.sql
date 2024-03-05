--use 7 days as an example
with user_pv as
(
    select id as user_id, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp) AS pv_ts
  from  APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER
),
disbursement as
(
    select * from ANALYTIC_DB.DBT_marts.disbursements
),
settlement as
(
    select * from ANALYTIC_DB.DBT_marts.settlements
),
totalrevenue as
(
    select
        EVENT_DS,
        USER_ID,
        SUBSCRIPTION_REVENUE,
        NET_BANDKING_REVENUE,
        INSTANT_WITHDRAWAL_REVENUE,
        NET_SURVEY_REVENUE
    from ANALYTIC_DB.DBT_metrics.total_dave_revenue
),
-- get all the disbursement after 7 days
user_disburse as (
    select
        user_pv.user_id,
        user_pv.pv_ts,
        dis.disbursement_ds_pst,
        dis.settement_due_ds_pst,
        dis.advance_id
    from user_pv
    left join disbursement dis
    on dis.user_id = user_pv.user_id
    and dis.DISBURSEMENT_DS_PST <= dateadd('day', user_pv.pv_ts, 7)
    WHERE dateadd('day', dis.SETTLEMENT_DS_PST, 30) <= current_date()
),
user_settlement as (
    select
        dis.*,
        stl.settled_30_days_after_due - stl.principal as net_credit_revenue
    from user_disburse dis
    left join settlement stl
    on dis.user_user_id = stl.user_id and dis.advance_id = stl.advance_id
),
other_revenue as (
    select
        up.user_id,
        up.pv_ts,
        tr.EVENT_DS,
        tr.SUBSCRIPTION_REVENUE,
        tr.NET_BANDKING_REVENUE,
        tr.INSTANT_WITHDRAWAL_REVENUE,
        tr.NET_SURVEY_REVENUE
    from user_pv up
    left join totalrevenue tr
    on up.user_id = tr.user_id
    and tr.EVENT_DS <= dateadd('day', up.pv_ts, 7)
),
credit_revenue as (
    select
        user_id,
        pv_ts,
        sum(net_credit_revenue) as credit_revenue_7_day
    from user_settlement
    group by 1, 2
),
other_revenue_sum as (
    select
        user_id,
        pv_ts,
        sum(SUBSCRIPTION_REVENUE) as SUBSCRIPTION_REVENUE_7_day,
        sum(NET_BANDKING_REVENUE) as NET_BANDKING_REVENUE_7_day,
        sum(INSTANT_WITHDRAWAL_REVENUE) as INSTANT_WITHDRAWAL_REVENUE_7_day,
        sum(NET_SURVEY_REVENUE) as NET_SURVEY_REVENUE_7_day
    from other_revenue
    group by 1, 2

),
7_summary as (
    select
        coalesce(cr.user_id, ors.user_id) as user_id,
        coalesce(cr.pv_ts, ors.pv_ts) as pv_ts,
        coalesce(cr.credit_revenue_7_day, 0) as credit_revenue_7_day,
        coalesce(ors.SUBSCRIPTION_REVENUE_7, 0)
        coalesce(ors.NET_BANDKING_REVENUE_7, 0)
        coalesce(ors.INSTANT_WITHDRAWAL_REVENUE_7)
        coalesce(ors.net_credit_revenue_7, 0)
    from credit_revenue cr
    full join
    other_revenue_sum ors
    on cr.user_id = ors.user_id
)
select * from 7_summary