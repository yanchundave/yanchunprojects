--create or replace table sandbox.dev_yyang.sub2_lastsweep as
with
bank_connection AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION

),

bank_connection_healthiness AS (
    -- healthiness of individual bank connections (at bank connection level)
    SELECT
        user_id,
        id AS connection_id,
        CASE
            WHEN banking_data_source_error_code in ('ITEM_NOT_FOUND',
                                                    'ACCESS_NOT_GRANTED',
                                                    'INSTITUTION_NOT_FOUND',
                                                    'INSTITUTION_NO_LONGER_SUPPORTED',
                                                    'ITEM_NOT_SUPPORTED',
                                                    'NO_ACCOUNTS',
                                                    'ITEM_LOCKED')
                THEN 3 -- 'Disconnected'
            WHEN has_valid_credentials = FALSE OR banking_data_source_error_code in ('INSUFFICIENT CREDENTIALS',
                                                    'INVALID_CREDENTIALS',
                                                    'INVALID_MFA',
                                                    'INVALID_SEND_METHOD',
                                                    'ITEM_LOGIN_REQUIRED',
                                                    'USER_SETUP_REQUIRED',
                                                    'MFA_NOT_SUPPORTED',
                                                    'INSUFFICIENT_CREDENTIALS')
                THEN 2 -- 'Unhealthy'
            ELSE 1 -- 'Healthy'
            END AS bc_healthiness_score
    FROM bank_connection
    WHERE deleted IS NULL
        AND _fivetran_deleted = false
        AND banking_data_source = 'PLAID'
),

user_bank_connection_healthiness AS (
    -- healthiness of bank connections at user level
    SELECT
        user_id,
        MIN(bc_healthiness_score) AS user_bc_healthiness_score
    FROM bank_connection_healthiness
    GROUP BY 1
),
disconnected as (
    select distinct user_id from user_bank_connection_healthiness where user_bc_healthiness_score = 3
),
unattempted as
(
    select user_id, bill_due_date
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date >= dateadd('day', -365, current_date())
    qualify row_number() over (partition by user_id order by bill_due_date desc) = 1
),
subscriber_new AS (
    SELECT
        distinct sub.USER_ID
    FROM DAVE.subscription.subscription sub
    JOIN DAVE.subscription.tier tier on sub.tier_id = tier.id
    WHERE tier.code in ('one_dollar', 'paywall_experiment', 'dogfooding_subs_2_0')
        AND  sub._DELETED = FALSE
),
allusers as (
    select
        a.user_id, a.bill_due_date
    from unattempted a
    left join subscriber_new b
    on a.user_id = b.user_id
    where b.user_id is null
    qualify row_number() over (partition by a.user_id order by a.bill_due_date desc) = 1
),
alluser_exclude_disconnect as
(
    select
        a.*
    from allusers a
    left join disconnected b
    on a.user_id = b.user_id
    where b.user_id is null
),
attempted_oct as
(
    select distinct USER_ID
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where FIRST_ATTEMPT_PT_DT >= DATE('2023-10-01')
    UNION
    SELECT DISTINCT USER_ID
    FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    WHERE CREATED >= DATE('2023-10-01') AND STATUS = 'COMPLETED'
),

approval as
(
    select distinct b.user_id
    from analytic_db.dbt_marts.fct_advance_approvals a
    join unattempted b
    on a.user_id = b.user_id
    where a.requested_ds >= dateadd('month', -12, current_date()) and a.is_approved = 1
),
mau as (

    select distinct try_to_number(user_id) as user_id
    from analytic_db.dbt_marts.amplitude_dao
    where EVENT_TYPE IN ('biometric confirmation success', 'passcode validate success', 'passcode signup success')
    and  date(event_time) >= dateadd('month', -12, current_date())
),
paused_user as (
    SELECT
        USER_ID, PAUSED_AT, UNPAUSED_AT
    FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.MEMBERSHIP_PAUSE
    WHERE PAUSED_AT <= CURRENT_DATE()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY PAUSED_AT DESC) = 1
),

paused_update as (
    SELECT distinct user_id
    FROM paused_user
    WHERE unpaused_at > current_date()
),
deleted_user as (
    select distinct user_id from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_BILLING
    where _FIVETRAN_DELETED = TRUE or DELETED = TRUE
    UNION
    SELECT DISTINCT ID AS user_id FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER
    where _FIVETRAN_DELETED = TRUE OR DELETED = TRUE
)
select

    a.user_id,
    a.bill_due_date,
    case
    when b.user_id is not null then 1
    else 0
    end as attempted_in_oct,
    case
    when c.user_id is not null then 1
    else 0
    end as approval_in_year,
    case
    when d.user_id is not null then 1
    else 0
    end as mau_in_year,
    case
    when e.user_id is not null then 1
    else 0
    end as if_paused,
    case
    when f.subscription_fee >0 and f.is_subscribed = TRUE then 0
    else 1
    end as if_user_paused,
    case
    when g.user_id is not null then 1
    else 0
    end as if_deleted,
    case
    when a.bill_due_date < dateadd('month', -3, current_date())
        then
            case
                when dayofweek(current_date()) < 6 then dateadd('day',  5 - dayofweek(current_date()), current_date())
                when dayofweek(current_date()) = 6 then dateadd('day', 7, current_date())
                else dateadd('day', 6, current_date())
             end
    else
          case
                when dayofmonth(a.bill_due_date) <= dayofmonth(current_date()) then dateadd('day', dayofmonth(a.bill_due_date) -1, date_trunc('month', dateadd('month', 1, current_date())))
                else dateadd('day', dayofmonth(a.bill_due_date) -1, date_trunc('month', current_date()))
          end
    end as next_bill_date
from alluser_exclude_disconnect a
left join attempted_oct b
on a.user_id = b.user_id
left join approval c
on a.user_id = c.user_id
left join mau d
on a.user_id = d.user_id
left join paused_update e
on a.user_id = e.user_id
left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER f
on a.user_id = f.id
left join deleted_user g
on a.user_id = g.user_id
