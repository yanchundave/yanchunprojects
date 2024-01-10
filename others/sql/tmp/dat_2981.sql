--All users table ANALYTIC_DB.DBT_MARTS.NEW_USER_ATTRIBUTION  19M
--bank connection with HAS_VALID_CREDENTIALS AND _fivetran_deleted = false  9.8M
--- paused APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.MEMBERSHIP_PAUSE
--- Has kyc
-- bank connection healthy
-- subscription canceled

with allusers as (
    select distinct user_id from ANALYTIC_DB.DBT_MARTS.NEW_USER_ATTRIBUTION where pv_ts is not null
),
bankconnect as (
    select count(distinct user_id)
 from APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION
 WHERE deleted IS NULL
    AND _fivetran_deleted = false
    AND banking_data_source = 'PLAID'
    and has_valid_credentials
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
pmupdate as (
    select user_id from paused_user where paused_user.value = 1
),
subsummary as (
    select
        a.user_id,
        case
            when b.user_id is null then 0
            else 1
        end as if_has_bank_credential,
        case
            when c.user_id is null then 1
            else 0
        end as if_not_paused

    from allusers a
    left join bankconnect b
    on a.user_id = b.user_id
    left join pmupdate c
    on a.user_id = c.user_id
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
kyc_success as (
    select dave_user_id as user_id from kyc where kyc_status = 'kyc_success'
),
subsummary_2 as (
    select
        a.*,
        case
        when b.user_id is null then 0
        else 1
        end as if_kyc_success
    from subsummary a
    left join kyc_success b
    on a.user_id = b.user_id
),
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
subsummary_3 as
(
    select
        a.*,
        case
            when b.user_id is null then 1
            else 0
        end as if_bank_healthy

    from subsummary_2 a
    left join disconnected b
    on a.user_id = b.user_id
),
applogin as
(
    select distinct try_to_number(user_id) as user_id from analytic_db.dbt_marts.amplitude_dao
    where EVENT_TYPE IN ('biometric confirmation success', 'passcode validate success', 'passcode signup success')
    and  date(event_time) >= dateadd('month', -6, current_date())
)
select count(*) from subsummary_3 where if_has_bank_credential > 0 and if_not_paused > 0 and if_kyc_success > 0 and if_bank_healthy > 0