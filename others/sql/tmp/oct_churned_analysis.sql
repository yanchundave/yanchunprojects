create or replace table sandbox.dev_yyang.oct_churned_users as
with octbill as (
    select * from sandbox.dev_yyang.legacy_oct23 where billing_cycle='2023-09'
),
sep_user as (
  select
  b.bill_due_date as bill_due_date, b.is_collect_succeeded ,  b.user_id
  from SANDBOX.DEV_ASCHMIDLI.subs_cohort_6 a
  join octbill b
  on a.id = b.user_id
  left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER c on a.id = c.id
  where c.subscription_fee = 0 and is_collect_succeeded = 1
  order by 1
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
        sc_charge.unit_cost
    FROM subscriber_new subscriber
    LEFT JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
    WHERE (sc_charge._DELETED = FALSE or sc_charge._DELETED IS NULL) and billing_cycle = date('2023-10-01')
),
churned as (
    select
        a.term_started,
        a.user_id,
        a.collect_status,
        a.payment_final,
        b.bill_due_date,
        b.is_collect_succeeded

    from sub_charge_new a
    join sep_user b
     on a.user_id = b.user_id
)
select * from churned


-----balance check

with sub_new as (
    select * from DAVE.subscription.subscription sub
),
tier_new AS
(
    SELECT * FROM DAVE.subscription.tier
),
user AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER

),
subscriber AS (
    SELECT
        distinct sub.USER_ID
    FROM sub_new sub
    JOIN tier_new tier on sub.tier_id = tier.id
    INNER JOIN user u on u.id = sub.user_id
    WHERE tier.code = 'one_dollar'
        AND  sub._DELETED = FALSE
)
select a.event_type, date(a.event_time) as event_date, count(*) as event_count, count(distinct a.user_id) as unique_users
from   analytic_db.dbt_marts.amplitude_dao as a
join subscriber b
on try_to_number(a.user_id) = b.user_id
where event_type is not null
and date(event_time) >= Date('2023-01-01')
and event_type like '%balance check%'
group by 1, 2

----Failure reason

select b.*, a.event_type, a.event_properties:reason
from sandbox.dev_yyang.oct_churned_users b
left join analytic_db.dbt_marts.amplitude_dao a
on try_to_number(a.user_id) = b.user_id
where a.event_type in (
'subscription ach failed',
'subscription ach not allowed',
'subscription ach rescheduled',
'subscription ach returned',
'subscription charge not allowed',
'subscription charged payment method invalid',
'subscription no payment method on file'
) and date(a.event_time) >= Date('2023-10-01')
qualify row_number() over (partition by b.user_id order by a.server_received_time desc) = 1



---- Sep churn rate
with aug as
 (
    select distinct user_id from sandbox.dev_yyang.legacy_oct23
   where billing_cycle = '2023-08' and is_collect_succeeded = 1
 ),
 sep as
 (
    select distinct user_id from sandbox.dev_yyang.legacy_oct23 where billing_cycle = '2023-09' and is_collect_succeeded = 1
 )
 select
 count(aug.user_id) as total_aug,
 sum(
    case
   when sep.user_id is null then 1
   else 0 end
 ) as churned

 from aug left join sep on aug.user_id = sep.user_id