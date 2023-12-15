create or replace table sandbox.dev_yyang.dec_last_sweep as
with allusers as
(
    select sb.user_id, max(sb.due_date) as last_due
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_BILLING sb
    inner join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER u on sb.user_id = u.id
    where u._fivetran_deleted = false and sb._fivetran_deleted = false and u.deleted = '9999-12-31 23:59:59+00:00'
    group by 1

),
sub2 as (
    select distinct user_id from DAVE.SUBSCRIPTION.SUBSCRIPTION
),
not_in_sub2 as (

    select
        a.*
    from allusers a
    left join sub2 s
    on a.user_id = s.user_id
    where s.user_id is null
),
pm as (
    select
        user_id,
        to_timestamp_ntz(PAUSED_AT) as time_value,
        1 AS VALUE
    from APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.MEMBERSHIP_PAUSE
    UNION ALL
    SELECT
        USER_ID,
        to_timestamp_ntz(UNPAUSED_AT) as time_value,
        -1 AS VALUE
    FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.MEMBERSHIP_PAUSE
),
paused_user as (
    select
        user_id,
        value,
        time_value
    from pm
    where time_value < CONVERT_TIMEZONE('America/Los_Angeles','UTC',CURRENT_TIMESTAMP()::timestamp_ntz)
    qualify row_number() over (partition by user_id order by time_value desc) = 1
),
cats as (
select
    a.*,
    b.time_value,
    case
    when b.value > 0 then 'paused'
    when b.value < 0 then 'unpaused'
    else 'others'
    end as label
from not_in_sub2 a
left join paused_user b
on a.user_id = b.user_id
),
kyc as (
    select * ,
    case
    when DECISION_TYPE_ID <> '144a1611fde111ea9b343741a3cc4b8e'  then 'kyc_success'
    else 'kyc_failed'
    end as kyc_status
    from DAVE.IDENTITY.USER_KYC_CHECK
    where DELETED IS NULL
    AND _DELETED=FALSE
    --AND DECISION_TYPE_ID <> '144a1611fde111ea9b343741a3cc4b8e'
    AND id is not null
    qualify row_number() over (partition by dave_user_id order by CREATED DESC) = 1
),
success as (
    select user_id, max(bill_due_date) as last_success_collect
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where is_collect_succeeded = 1
    group by 1

),
final as (
select
    a.*,
    case
    when b.dave_user_id is not null then b.kyc_status
    else 'kyc_no_information'
    end as kyc_info,
    case
    when c.user_id is not null then last_success_collect
    else null
    end as last_success_time
from cats a
left join kyc b
on a.user_id = b.dave_user_id
left join success c
on a.user_id = c.user_id

)
select * from final