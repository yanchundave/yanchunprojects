---Debt return due to insufficient funding
with test as (
select t.referenceid, ft.davecode
from dave.loomis.transaction as t
join dave.loomis.failedtransaction as ft on t.transactionid = ft.transactionid
where t.originatingprocess = 'subscription'
and ft.davecode = 'insufficient-funds'
and t.created >= date('2023-09-12')
),
sub as (
    select * from DAVE.subscription.subscription sub
),
charge as (
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
cohort as (
    select * from sandbox.dev_yyang.sub_cohort5
)
select
cohort.id as user_id, sub.id as subscription_id, charge.id as subscription_charge_id, a.id as attempt_id
from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE_attempt a
join test on a.reference_id = test.referenceid
join charge on a.subscription_charge_id = charge.id
join sub on charge.subscription_id = sub.id
join cohort on sub.user_id = cohort.id

-----Sub2 balance check through Amplitude dao

with userlist as (
  select * from sandbox.dev_yyang.sub_cohort5
)
select a.event_type, b.label, count(*) as event_count, count(distinct a.user_id) as unique_users
from   analytic_db.dbt_marts.amplitude_dao as a
join userlist b
on try_to_number(a.user_id) = b.id
where event_type is not null
and date(event_time) >= Date('2023-09-12')
and date(event_time) <= DATE('2023-10-18')
and event_type like '%balance check%'
group by 1, 2

-----legacy cohort query
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
users as
(
    select
        b.id as user_id,
        a.bill_id,
        a.bill_due_date,
        a.payment_method,
        a.is_collect_succeeded,
        a.billing_cycle
    from  sandbox.dev_yyang.sub_cohort5 b
    LEFT JOIN ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS a
    on a.user_id = b.id
    where a.billing_cycle = '2023-09' and b.label = 'legacy'


),
attempts as (
    select
        id as attempt_id,
        created,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) as created_pst,
        subscription_billing_id,
         parse_json(extra):chargeType as chargeType,
        subscription_payment_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) >= DATE('2023-09-12')
    and CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) <= DATE('2023-10-15')
),
payments as (
    select
        a.attempt_id,
        a.created_pst,
        a.subscription_billing_id,
        a.chargeType,
        b.status,
        b.external_processor,
        b.id as payment_id,
        case
            when b.id is null then
                case
                    when chargeType like 'debit charge%'
                        OR chargeType like '%forced debit only%' then 'DEBIT'
                    WHEN chargeType like 'bank charge%'
                        or chargeType like 'balance is above%' then 'ACH'
                    ELSE 'OTHER'
                END
            WHEN b.id is not null then
                case
                    WHEN external_processor like 'SYNAPSEPAY' THEN 'ACH'
                    WHEN external_processor like 'TABAPAY' THEN 'DEBIT'
                    ELSE 'other'
                end
            else 'others'
        end as processor

    from attempts a
    left join APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT b
    on a.subscription_payment_id = b.id

),
final as (
    select
      a.user_id,
      a.bill_id,
      a.bill_due_date,
      a.payment_method as payment_final,
      a.billing_cycle,
      b.attempt_id,
      b.created_pst,
      date(b.created_pst) AS attempted_date,
      b.chargeType,
      b.status,
      b.external_processor,
      b.payment_id,
      b.processor as payment_attempt,
      c.user_bc_healthiness_score
    from  users a
    left join payments b
    on a.bill_id = b.subscription_billing_id
    left join user_bank_connection_healthiness c
    on a.user_id = c.user_id
)
select * from final

---sub2 cohort
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
  and date(t.created) >= date('2023-09-12')
),
subscriber_new AS (
    SELECT
        sub.USER_ID,
        sub.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id
    from sub_new sub
    JOIN tier_new tier
    ON sub.tier_id = tier.id
    JOIN sandbox.dev_yyang.sub_cohort5 b
    ON sub.user_id = b.id
    WHERE tier.code = 'one_dollar'
        AND  sub._DELETED = FALSE and b.label='sub2'
),
candidates as
(
  select
    distinct user_id
  from subscriber_new
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
    WHERE billing_cycle = DATE('2023-09-01')
),
last_attempt_new as (
    select
        subscription_charge_id,
        date(max(created)) as last_attempt_date
    from attempts_new
    group by 1
),
sub_attempts_new AS (
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
        debit.referenceid
    FROM sub_charge_new s
    LEFT JOIN  attempts_new a on a.subscription_charge_id = s.subscription_charge_id
    LEFT JOIN payment_method payment ON a.payment_method_kind_id = payment.id
    LEFT JOIN last_attempt_new la on la.subscription_charge_id = s.subscription_charge_id
    LEFT JOIN debit on debit.referenceid = a.reference_id
    WHERE a.created >= date('2023-09-12')
    AND a.created <= DATE('2023-10-15')
    order by monthnum, attempt_date, created
),
final_new as (
    select
        sn.user_id as user_id,
        a.* exclude (user_id),
        c.user_bc_healthiness_score
    from candidates sn
    left join sub_attempts_new a
    --join sub_attempts_new a
    on sn.user_id = a.user_id
    left join user_bank_connection_healthiness  c
    on sn.user_id = c.user_id
)
select * from final_new

