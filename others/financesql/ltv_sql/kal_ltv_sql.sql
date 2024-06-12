create or replace table sandbox.dev_ktodorov.ltv_training_240520 as
with user_sample as (
    select
        user_id
        -- Observation timestamp is the timestamp as of which the independent variables are calculated and is also the start of the 6/12-mo period over which the dependent is calculated
        -- When running model prediction, the observation datetime is the same as datetime the forecast is being ran on
        , dateadd(hour, abs(random(42)) % datediff(hour, phone_verified_ts_pst, current_timestamp()), phone_verified_ts_pst) as observation_ts -- Random date between pv_date and the max observation date
        , date_trunc(day, phone_verified_ts_pst) as pv_date
        , platform
        , attribution as acquisition_channel
        , network as acquisition_network
        , case
            when State in ('CT', 'ME', 'MA', 'NH', 'RI', 'VT') then 'New England'
            when State in ('DE', 'DC', 'MD', 'NJ', 'NY', 'PA') then 'Mideast'
            when State in ('IL', 'IN', 'MI', 'OH', 'WI') then 'Great Lakes'
            when State in ('IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') then 'Plains'
            when State in ('AL', 'AR', 'FL', 'GA', 'KY', 'LA', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV') then 'Southeast'
            when State in ('AZ', 'NM', 'OK', 'TX') then 'Southwest'
            when State in ('CO', 'ID', 'MT', 'UT', 'WY') then 'Rocky Mountain'
            when State in ('AK', 'CA', 'HI', 'NV', 'OR', 'WA') then 'Far West'
            else 'Other' end
            as region -- Region classification as per the BEA
    from
        analytic_db.dbt_marts.dim_users
    where
        datediff(day, phone_verified_ts_pst, current_timestamp()) > 0
    order by sha2(user_id)
        limit 10000000
)
, advances_with_settlement as (
    select
        disbursements.advance_approval_id
        , disbursements.user_id
        , disbursement_ts_pst
        , fct_advance_approvals_dedup.bank_account_id
        , datediff(days, phone_verified_ts_pst, disbursement_ts_pst) as user_age_at_disbursement
        , settlement_due_ds_pst
        , max_approved_amount
        , taken_amount
        , tip_amount
        , pledged_revenue
        , taken_amount + pledged_revenue as amount_due
        , disbursement_method
        , total_settled_amount
        , case when total_settled_amount = amount_due then pledged_revenue else amount_due - total_settled_amount end as revenue
        , settlement_due_ds_pst > current_timestamp() as outstanding_today
    from
        analytic_db.dbt_marts.disbursements
        left join analytic_db.dbt_marts.fct_advance_approvals_dedup using (advance_approval_id)
        left join analytic_db.dbt_marts.dim_users on dim_users.user_id = disbursements.user_id
        left join analytic_db.dbt_marts.settlements using (advance_id)
)
, prior_advances as (
    select
        a.*
        , observation_ts
        , settlement_due_ds_pst > observation_ts as outstanding_at_observation
    from
        advances_with_settlement as a
        join user_sample as b using (user_id)
    where
        disbursement_ts_pst <= observation_ts
)
, advance_lifetime as (
    select
        user_id
        , count(*) as "count"
        , sum(taken_amount) as amount_total
        , avg(taken_amount) as amount_mean
        , max(taken_amount) - min(taken_amount) as amount_range
        , stddev(taken_amount) as amount_stddev
        , regr_slope(taken_amount, user_age_at_disbursement) as amount_slope
        , case when sum(taken_amount) > 0 then sum(tip_amount) / sum(taken_amount) else null end as tip_percent_mean
        , sum(revenue) as revenue_total
        , avg(revenue) as revenue_mean
        , max(revenue) - min(revenue) as revenue_range
        , stddev(revenue) as revenue_stddev
        , regr_slope(revenue, user_age_at_disbursement) as revenue_slope
        , max(case when revenue < 0 then 1 else 0 end) as has_default
        , min(disbursement_ts_pst) as first_disbursement_ts
        , avg(iff(disbursement_method = 'Spending', 1, 0)) as disbursement_spending_pct
        , avg(iff(disbursement_method = 'Debit', 1, 0)) as disbursement_debit_pct
        , avg(iff(disbursement_method = 'ACH', 1, 0)) as disbursement_ach_pct
        , count(distinct bank_account_id) as bank_account_count
    from
        prior_advances
    group by
        1
)
, advance_last_6m as (
    select
        user_id
        , count(*) as "count"
        , sum(taken_amount) as amount_total
        , avg(taken_amount) as amount_mean
        , max(taken_amount) - min(taken_amount) as amount_range
        , stddev(taken_amount) as amount_stddev
        , regr_slope(taken_amount, user_age_at_disbursement) as amount_slope
        , case when sum(taken_amount) > 0 then sum(tip_amount) / sum(taken_amount) else null end as tip_percent_mean
        , sum(revenue) as revenue_total
        , avg(revenue) as revenue_mean
        , max(revenue) - min(revenue) as revenue_range
        , stddev(revenue) as revenue_stddev
        , regr_slope(revenue, user_age_at_disbursement) as revenue_slope
        , avg(iff(disbursement_method = 'Spending', 1, 0)) as disbursement_spending_pct
        , avg(iff(disbursement_method = 'Debit', 1, 0)) as disbursement_debit_pct
        , avg(iff(disbursement_method = 'ACH', 1, 0)) as disbursement_ach_pct
    from
        prior_advances
    where
        disbursement_ts_pst >= dateadd(month, -6, observation_ts)
    group by
        1
)
, advance_last as (
    select
        user_id
        , taken_amount as amount
        , iff(taken_amount > 0, tip_amount / taken_amount, null) as tip_percent
        , revenue as revenue
        , datediff(days, disbursement_ts_pst, observation_ts) as recency
        , outstanding_at_observation as outstanding
        , iff(outstanding_at_observation, amount_due, null) as outstanding_due
        , iff(outstanding_at_observation, pledged_revenue, null) as outstanding_pledged_revenue
        , disbursement_method as disbursement_method
    from
        prior_advances
    qualify
        (row_number() over (partition by user_id order by disbursement_ts_pst desc)) = 1
)
, prior_approval as (
    select
        user_id
        , observation_ts
        , requested_ts
        , datediff(days, pv_date, requested_ts) as user_age_at_request
        , is_approved
        , max_approved_amount as approved_amount
        , primary_rejection_reason
    from
        user_sample
        left join analytic_db.dbt_marts.fct_advance_approvals_dedup using (user_id)
    where
        requested_ts <= observation_ts
        and pre_approval = 'Natural Approval'
)
, approval_lifetime as (
    select
        user_id
        , count(*) as request_count
        , sum(is_approved::number) as approval_count
        , avg(is_approved::number) as approval_rate
        , avg(approved_amount) as amount_mean
        , max(approved_amount) - min(approved_amount) as amount_range
        , stddev(approved_amount) as amount_stddev
        , regr_slope(approved_amount, user_age_at_request) as amount_slope
    from
        prior_approval
    group by
        1
)
, approval_last_6m as (
    select
        user_id
        , count(*) as request_count
        , sum(is_approved::number) as approval_count
        , avg(is_approved::number) as approval_rate
        , avg(approved_amount) as amount_mean
        , max(approved_amount) - min(approved_amount) as amount_range
        , stddev(approved_amount) as amount_stddev
        , regr_slope(approved_amount, user_age_at_request) as amount_slope
    from
        prior_approval
    where
        requested_ts >= dateadd(month, -6, observation_ts)
    group by
        1
)
, approval_last as (
    select
        user_id
        , datediff(day, requested_ts, observation_ts) as recency
        , is_approved
        , approved_amount
        , primary_rejection_reason as rejection_reason
    from
        prior_approval
    qualify
        (row_number() over (partition by user_id order by requested_ts desc)) = 1
)
, approval_rejection as (
    select
        user_id
        , mode(primary_rejection_reason) as rejection_reason_mode
    from
        prior_approval
    where
        not is_approved
    group by
        1
)
, bank_connection as (
    select
        user_sample.user_id
        , count(*) as "count"
        , boolor_agg(has_valid_credentials) as has_valid_credentials
        , boolor_agg(has_transactions) as has_transactions
    from
        user_sample
        left join application_db.google_cloud_mysql_dave.bank_connection using (user_id)
    where
        created <= observation_ts
        and updated <= observation_ts
        and deleted is null
    group by
        1
)
, sessions as (
    select
        user_sample.user_id
        , count(*) as lifetime_count
        , sum(iff(datediff(month, event_ds_pst, observation_ts) <= 6, 1, 0)) as last_6m_count
        , sum(iff(datediff(month, event_ds_pst, observation_ts) <= 1, 1, 0)) as last_1m_count
        , min(datediff(day, event_ds_pst, observation_ts)) as recency
    from
        user_sample
        left join analytic_db.dbt_marts.fct_user_sessions using (user_id)
    where
        event_ds_pst <= observation_ts
    group by
        1
)
, labels as ( -- the advances and revenue AFTER observation_ts
    select
        user_sample.user_id
        , sum(iff(settlement_due_ds_pst <= dateadd(month, 6, observation_ts) and not outstanding_today, revenue, 0)) as revenue_6m
        , sum(iff(settlement_due_ds_pst <= dateadd(month, 12, observation_ts) and not outstanding_today, revenue, 0)) as revenue_12m
    from
        user_sample
        left join advances_with_settlement using (user_id)
    group by
        1
)
select
    user_sample.*
    , datediff(day, pv_date, observation_ts) as user_age

    , coalesce(nua.bod_account_open_user::boolean, false) as bod_account_open
    , coalesce(nua.bod_direct_deposit_user::boolean, false) as bod_direct_deposit

    , datediff(day, '2023-9-1', observation_ts) as days_since_percentage_based_pricing

    , coalesce(advance_lifetime."count", 0) as advance_lifetime_count
    , advance_lifetime.amount_total as advance_lifetime_amount_total
    , advance_lifetime.amount_mean as advance_lifetime_amount_mean
    , advance_lifetime.amount_range as advance_lifetime_amount_range
    , advance_lifetime.amount_stddev advance_lifetime_amount_stddev
    , advance_lifetime.amount_slope as advance_lifetime_amount_slope
    , advance_lifetime.tip_percent_mean as advance_lifetime_tip_percent_mean
    , advance_lifetime.revenue_total as advance_lifetime_revenue_total
    , advance_lifetime.revenue_mean as advance_lifetime_revenue_mean
    , advance_lifetime.revenue_range as advance_lifetime_revenue_range
    , advance_lifetime.revenue_stddev as advance_lifetime_revenue_stddev
    , advance_lifetime.revenue_slope as advance_lifetime_revenue_slope
    , advance_lifetime.has_default as advance_lifetime_has_default
    , advance_lifetime.disbursement_spending_pct as advance_lifetime_disbursement_spending_pct
    , advance_lifetime.disbursement_debit_pct as advance_lifetime_disbursement_debit_pct
    , advance_lifetime.disbursement_ach_pct as advance_lifetime_disbursement_ach_pct
    , advance_lifetime.bank_account_count as advance_lifetime_bank_account_count

    , datediff(day, pv_date, advance_lifetime.first_disbursement_ts) as advance_first_days_from_pv
    , datediff(day, advance_lifetime.first_disbursement_ts, observation_ts) as advance_first_recency

    , coalesce(advance_last_6m."count", 0) as advance_last_6m_count
    , advance_last_6m.amount_total as advance_last_6m_amount_total
    , advance_last_6m.amount_mean as advance_last_6m_amount_mean
    , advance_last_6m.amount_range as advance_last_6m_amount_range
    , advance_last_6m.amount_stddev advance_last_6m_amount_stddev
    , advance_last_6m.amount_slope as advance_last_6m_amount_slope
    , advance_last_6m.tip_percent_mean as advance_last_6m_tip_percent_mean
    , advance_last_6m.revenue_total as advance_last_6m_revenue_total
    , advance_last_6m.revenue_mean as advance_last_6m_revenue_mean
    , advance_last_6m.revenue_range as advance_last_6m_revenue_range
    , advance_last_6m.revenue_stddev as advance_last_6m_revenue_stddev
    , advance_last_6m.revenue_slope as advance_last_6m_revenue_slope
    , advance_last_6m.disbursement_spending_pct as advance_last_6m_disbursement_spending_pct
    , advance_last_6m.disbursement_debit_pct as advance_last_6m_disbursement_debit_pct
    , advance_last_6m.disbursement_ach_pct as advance_last_6m_disbursement_ach_pct

    , advance_last.amount as advance_last_amount
    , advance_last.tip_percent as advance_last_tip_percent
    , advance_last.revenue as advance_last_revenue
    , advance_last.recency as advance_last_recency
    , advance_last.outstanding as advance_last_outstanding
    , advance_last.outstanding_due as advance_last_outstanding_due
    , advance_last.outstanding_pledged_revenue as advance_last_outstanding_pledged_revenue
    , advance_last.disbursement_method as advance_last_disbursement_method

    , coalesce(approval_lifetime.request_count, 0) as approval_lifetime_request_count
    , coalesce(approval_lifetime.approval_count, 0) as approval_lifetime_approval_count
    , approval_lifetime.approval_rate as approval_lifetime_approval_rate
    , approval_lifetime.amount_mean as approval_lifetime_amount_mean
    , approval_lifetime.amount_range as approval_lifetime_amount_range
    , approval_lifetime.amount_stddev as approval_lifetime_amount_stddev
    , approval_lifetime.amount_slope as approval_lifetime_amount_slope

    , coalesce(approval_last_6m.request_count, 0) as approval_last_6m_request_count
    , coalesce(approval_last_6m.approval_count, 0) as approval_last_6m_approval_count
    , approval_last_6m.approval_rate as approval_last_6m_approval_rate
    , approval_last_6m.amount_mean as approval_last_6m_amount_mean
    , approval_last_6m.amount_range as approval_last_6m_amount_range
    , approval_last_6m.amount_stddev as approval_last_6m_amount_stddev
    , approval_last_6m.amount_slope as approval_last_6m_amount_slope

    , approval_last.recency as approval_last_recency
    , approval_last.is_approved as approval_last_is_approved
    , approval_last.approved_amount as approval_last_approved_amount
    , approval_last.rejection_reason as approval_last_rejection_reason

    , approval_rejection.rejection_reason_mode as approval_rejection_reason_mode

    , coalesce(bank_connection."count", 0) as bank_connection_count
    , bank_connection.has_valid_credentials as bank_connection_has_valid_credentials
    , bank_connection.has_transactions as bank_connection_has_transactions

    , sessions.lifetime_count as session_lifetime_count
    , sessions.last_6m_count as session_last_6m_count
    , sessions.last_1m_count as session_last_1m_count
    , sessions.recency as session_recency

    -- Labels/Dependent variables
    , datediff(month, observation_ts, current_timestamp()) as label_months_of_data -- Number of months that are available to the labels
    , labels.revenue_6m > 0.1 as label_retention_6m
    , labels.revenue_12m > 0.1 as label_retention_12m
    , labels.revenue_6m as label_revenue_6m
    , labels.revenue_12m as label_revenue_12m

    , iff(mod(md5_number_lower64(user_id), 10) < 1, 'test', 'train') as dataset
from
    user_sample
    left join analytic_db.dbt_marts.new_user_attribution as nua using (user_id)
    left join advance_lifetime using (user_id)
    left join advance_last_6m using (user_id)
    left join advance_last using (user_id)
    left join approval_lifetime using (user_id)
    left join approval_last_6m using (user_id)
    left join approval_last using (user_id)
    left join approval_rejection using (user_id)
    left join bank_connection using (user_id)
    left join sessions using (user_id)
    left join labels using (user_id)
order by
    sha2(user_id)