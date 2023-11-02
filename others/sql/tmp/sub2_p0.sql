WITH
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
sub_new as (
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
payment_method AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.PAYMENT_METHOD_KIND
),
ach AS
(
  SELECT * FROM dave.ach.ach_transfer where description = 'DaveSubFee' and returned_on is not null
),
debit as
(
  select t.referenceid
  from dave.loomis.transaction as t
  join dave.loomis.failedtransaction as ft on t.transactionid = ft.transactionid
  where t.originatingprocess = 'subscription'
  and ft.davecode = 'insufficient-funds'
),
subscriber_new AS (
    SELECT
        sub.USER_ID,
        sub.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id
    from sub_new sub
    JOIN tier_new tier
    ON sub.tier_id = tier.id
    WHERE tier.code = 'one_dollar'
        AND  sub._DELETED = FALSE
),
sub_charge_new AS (
    SELECT
        DATE_TRUNC('MONTH',  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) AS billing_cycle,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) as term_started,
        subscriber.user_id,
        sc_charge.id AS subscription_charge_id,
        sc_status.code as collect_status,
        payment.kind as payment_final,
        sc_charge.unit_cost,
        ach.returned_on,
        ach.return_code
    FROM subscriber_new subscriber
    LEFT JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
    LEFT JOIN ach on sc_charge.reference_id = ach.reference_id
    WHERE sc_charge._DELETED = FALSE or sc_charge._DELETED IS NULL
),
last_attempt_new as (
    select
        subscription_charge_id,
        date(max(created)) as last_attempt_date
    from attempts_new
    group by 1
),
sub_attempts AS (
    SELECT
        s.billing_cycle,
        a.id as subscription_attempt_id,
        s.subscription_charge_id,
        s.user_id,
        a.created,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED)) AS attempt_date,
        DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED))) as monthnum,
        s.payment_final as payment_final,
        s.term_started,
        s.unit_cost,
        s.collect_status,
        payment.kind as payment_attempt,
        la.last_attempt_date,
        s.returned_on,
        s.return_code,
        debit.referenceid,
        case
        when la.last_attempt_date is not null and la.last_attempt_date = term_started and collect_status = 'collected' then 1
        else 0
        end as collected_at_bill_date,
        case
        when attempt_date is not null and attempt_date = term_started then 1
        else 0
        end as is_attempted_at_bill_date
    FROM sub_charge_new s
    LEFT JOIN  attempts_new a on a.subscription_charge_id = s.subscription_charge_id
    LEFT JOIN payment_method payment ON a.payment_method_kind_id = payment.id
    LEFT JOIN last_attempt_new la on la.subscription_charge_id = s.subscription_charge_id
    LEFT JOIN debit on debit.referenceid = a.reference_id
    WHERE  s.subscription_charge_id is NULL or s.term_started >= date('2022-01-01')
    order by monthnum, attempt_date, created
)
select * from sub_attempts