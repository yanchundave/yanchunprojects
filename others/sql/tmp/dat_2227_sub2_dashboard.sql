--- Balance check

with userlist as (
  select * from sandbox.dev_yyang.sub_cohort4
)
select a.event_type, b.label, count(*) as event_count, count(distinct a.user_id) as unique_users
from   analytic_db.dbt_marts.amplitude_dao as a
join userlist b
on try_to_number(a.user_id) = b.id
where event_type is not null
and date(event_time) >= Date('2023-08-16')
and date(event_time) <= DATE('2023-09-02')
and event_type like '%balance check%'
group by 1, 2


---Total Subscribers day, weekly and monthly
---Broken by new and existing
---Broken by churn and non-churn

with
recursive_attempt_date as
(
    select
        Date_trunc('month', CURRENT_DATE() - 365) as start_date,
        current_date() as end_date
),
cte_attempt_date(ref_date) as
(
    select
        start_date as ref_date
        from recursive_attempt_date
        union all
        select
        ref_date + 1 as ref_date
        from cte_attempt_date
        cross join
        recursive_attempt_date
        where ref_date < end_date
),
time_unit as
(
    select
      ref_date as x_date
    from cte_attempt_date
),
tier AS
(
    SELECT * FROM DAVE.subscription.tier
),
subscriber as
(
    select * from DAVE.subscription.subscription
),
user AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER

),
one_dave_new_members AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_new_members

),
alluser as (
    select
        time_unit.x_date as x_date,
        od.event_ds as reg_date,
        subscriber.USER_ID as user_id,
        subscriber.canceled,
        subscriber.ended,
        case
            when time_unit.x_date = date(od.event_ds) then 1
            when subscriber.canceled is null or subscriber.ended is null then 2
            else 3
        end as userclass
    from time_unit
    left join subscriber
    on time_unit.x_date >= CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', subscriber.started)
    JOIN tier tier
    ON subscriber.tier_id = tier.id
    INNER JOIN user u on u.id = subscriber.user_id
    LEFT JOIN one_dave_new_members od on u.id = od.user_id
    WHERE tier.code = 'one_dollar'
        AND  subscriber._DELETED = FALSE
),
allusergroup as
(
    select
        x_date,
        date_trunc('month', x_date) as month,
        date_trunc('week', x_date) as week,
        sum(
            case
                when userclass = 1 then 1
                else 0
            end
        ) as newusers_vol,
        sum(
            case
                when userclass = 2 then 1
                else 0
            end
        ) as churneduser_vol,
        sum(
            case
                when userclass = 3 then 1
                else 0
            end
        ) as existinguser_vol

    from alluser
    group by 1, 2, 3
)
select * from allusergroup


----- Include attempt, subscriber, success and ach and debit information
------
-------

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
  and date(t.created) >= date('2023-08-16')
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

---attempt count
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
    WHERE billing_cycle = DATE('2023-08-01')
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
    WHERE a.created >= date('2022-01-01')
    AND a.created <= current_date()
    order by monthnum, attempt_date, created
),
final_new as (
    select
        sn.user_id,
        sn.term_started,
        a.* exclude (user_id, term_started)
    from sub_charge_new sn
    left join sub_attempts_new a
    on sn.user_id = a.user_id
)
select * from final_new