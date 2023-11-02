with
migrated as
(
    select distinct user_id from DAVE.subscription.subscription
    union
    select id as user_id from SANDBOX.DEV_ASCHMIDLI.subs_cohort_6
    union
    select user_id from sandbox.dev_aschmidli.subs_october_cohort
    union
    select user_id from sandbox.dev_yyang.users_inlegacy
),
users_left as (
    select
    a.*
    from sandbox.dev_aschmidli.users_bank_connection_updated a
    left join migrated b
    on a.user_id = b.user_id
    where b.user_id is null
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

paused_user as (
    SELECT
        USER_ID, PAUSED_AT, UNPAUSED_AT
    FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.MEMBERSHIP_PAUSE
    WHERE PAUSED_AT <= CURRENT_DATE()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY PAUSED_AT DESC) = 1
),

paused_update as (
    SELECT *
    FROM paused_user
    WHERE unpaused_at > current_date()
),

oct_billing_cycle as (

   SELECT *
   FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_BILLING
   WHERE billing_cycle = '2023-10'
)

SELECT
    a.user_id,
    case
    when b.user_id is null then 'not_paused'
    else 'paused'
    end as if_paused_user,

    b.paused_at,

    case
    when c.user_id is null then 'no_billing_cycle_oct'
    else 'billing_cycle_oct'
    end as if_billing_cycle_oct,

    case
    when d.deleted <= current_date() or d.subscription_fee = 0 then 'deleted_or_fee_zero'
    else 'not_deleted'
    end as if_deleted_or_sub_cancel,

    e.user_bc_healthiness_score as bank_connection_healthy,

    case
    when e.user_bc_healthiness_score = 1 then 'healthy'
    when e.user_bc_healthiness_score = 2 then 'unhealthy'
    when e.user_bc_healthiness_score = 3 then 'disconnect'
    else 'others'
    end as healthy_or_unhealthy

FROM users_left a
LEFT JOIN paused_update b
ON a.user_id = b.user_id
LEFT JOIN oct_billing_cycle c
ON a.user_id = c.user_id
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER d
ON a.user_id = d.id
LEFT JOIN user_bank_connection_healthiness e
ON a.user_id = e.user_id