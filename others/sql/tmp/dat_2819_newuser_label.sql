---Identify real new users or false new users
create or replace table sandbox.dev_yyang.newusers_label as
WITH
users as (
    select ID, created
    from APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER
    where deleted > current_date()
),
bank_connection AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION

),

bank_earliest AS (
    -- healthiness of individual bank connections (at bank connection level)
    SELECT
        user_id,
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
            END AS bc_healthiness_score,  --Need verify whether unhealthy bc trigger new users created in sub
        created
    FROM bank_connection
    WHERE deleted IS NULL
        AND _fivetran_deleted = false
        AND banking_data_source = 'PLAID'
),
bankupdate as (
    select user_id, min(created) as earliest_bank_connect
    from bank_earliest
    where bc_healthiness_score = 1
    group by 1
),

sub2_last_attempt AS (
    SELECT
        SUBSCRIPTION_CHARGE_ID,
        MAX(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::TIMESTAMP_NTZ)) AS latest_date
    FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE_ATTEMPT
    GROUP BY 1
),

sub2 AS (
    SELECT DISTINCT
        TO_VARCHAR(DATE(COALESCE(lca.latest_date , CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started::TIMESTAMP_NTZ))), 'YYYY-MM') AS payment_month,
        sub.user_id,
        'sub2' AS sub_version,
        COALESCE(sc.UNIT_COST,0) / 100 AS revenue_collected
    FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE sc
    LEFT JOIN DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE_STATUS  ss
      ON sc.subscription_charge_status_id = ss.id
    LEFT JOIN sub2_last_attempt lca
      ON sc.id = lca.SUBSCRIPTION_CHARGE_ID
    LEFT JOIN DAVE.SUBSCRIPTION.SUBSCRIPTION sub
      ON sc.subscription_id = sub.id
    WHERE ss.code = 'collected'
      AND payment_month >='2019-01'
)

SELECT a.payment_month, a.user_id, b.earliest_bank_connect, c.created as user_created,
case
when date_trunc('month',date(b.earliest_bank_connect)) < date('2023-10-01') then 1
else 0
end as if_bank_created_before,
case
when date_trunc('month', date(c.created)) < date('2023-10-01') then 1
else 0
end as if_user_created_before
FROM sub2 a
left join bankupdate b
on a.user_id = b.user_id
left join users c
on a.user_id = c.id
WHERE payment_month = '2023-10'
    AND revenue_collected = 0