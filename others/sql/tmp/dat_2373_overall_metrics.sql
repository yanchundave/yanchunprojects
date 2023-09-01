"""billing_cycle,
is_prev_billing_cycle,
bill_due_date,
is_existing_subscriber,
bc_healthiness_score,
has_debit_card,
attempted_collect,
is_collect_succeeded,
payment_month,
payment_method,
payment_pt_dt,
----
first_trigger_error_message,
first_trigger_error_name,
first_trigger_failures,
total_trigger_count,
balance_bucket,
----

advance_user_segment,
user_cnt
"""

with sub_new as (
    select * from DAVE.subscription.subscription sub
),
sc_charge_new as (
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
attempts_new as (
    select * from DAVE.SUBSCRIPTION.subscription_charge_attempt
),
sc_status_new as (
    SELECT * FROM DAVE.SUBSCRIPTION.subscription_charge_status
),
tier_new AS
(
    SELECT * FROM DAVE.subscription.tier
),
user AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER

),

payment_method AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.PAYMENT_METHOD_KIND
),

debit_payment_method AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.PAYMENT_METHOD

),
one_dave_new_members AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_new_members

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

debit_card_user AS (
    -- users who have valid debit cards
    SELECT
        DISTINCT user_id
    FROM debit_payment_method
    WHERE INVALID IS NULL
          AND DELETED IS NULL
          AND EXPIRATION > CURRENT_DATE()
          AND _FIVETRAN_DELETED = false
),



subscriber_new AS (
    SELECT
        sub.USER_ID,
        sub.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id,
        od.event_ds as reg_date,
        TO_VARCHAR(od.EVENT_DS, 'YYYY-MM') as reg_month
    FROM sub_new sub
    JOIN tier_new tier on sub.tier_id = tier.id
    INNER JOIN user u on u.id = sub.user_id
    LEFT JOIN one_dave_new_members od on u.id = od.user_id
    WHERE tier.code = 'one_dollar'
        AND  sub._DELETED = FALSE
),

sub_charge_new AS (
    SELECT
        DATE_TRUNC('MONTH',  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) AS billing_cycle,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) as term_started,
        subscriber.user_id,
        subscriber.subscription_id,
        subscriber.reg_date,
        CASE WHEN reg_date < sc_charge.term_started THEN 1 ELSE 0 END AS is_existing_subscriber,
        sc_charge.id AS subscription_charge_id,
        sc_status.code as collect_status,
        sc_charge.unit_cost,
        payment.kind as payment_method
    FROM subscriber_new subscriber
    LEFT JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
),



---attempt count
sub_attempts_new AS (
    SELECT
        a.id as subscription_attempt_id,
        s.subscription_charge_id,
        a.created
    FROM attempts_new a
    JOIN sub_charge_new s on a.subscription_charge_id = s.subscription_charge_id
),

attempt_group_new as
(
    select
        subscription_charge_id,
        date(min(created)) as first_attempt_pt_dt,
        date(max(created)) as last_attempt_dt,
        count(distinct subscription_attempt_id) as attempt_cnt
    from sub_attempts_new
    group by 1

),

sub2 as (

    select
        TO_VARCHAR(s.billing_cycle, 'YYYY-MM') as billing_cycle,
        s.user_id,
        s.subscription_charge_id,
        s.term_started,
        s.reg_date,
        s.is_existing_subscriber,
        u.user_bc_healthiness_score as bc_healthiness_score,
        iff(d.user_id is not null, 1, 0) as has_debit_card,
        iff(ag.subscription_charge_id is not null, 1, 0) as attempted_to_collect,
        iff(s.collect_status= 'collected', 1, 0) as is_collect_succeeded,
        ag.first_attempt_pt_dt,
        case when s.collect_status = 'collected' then ag.last_attempt_dt else null end as payment_pt_date,
        coalesce(ag.attempt_cnt, 0) as attempt_cnt,
        s.payment_method
    from sub_charge_new s
    left join attempt_group_new as ag on s.subscription_charge_id = ag.subscription_charge_id
    left join user_bank_connection_healthiness u on s.user_id = u.user_id
    left join debit_card_user d on s.user_id = d.user_id

)
select * from sub2

