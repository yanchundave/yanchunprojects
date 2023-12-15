create or replace table sandbox.dev_yyang.sub1_tmp as
with
subscription_billing AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_BILLING

),

user AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER

),

one_dave_new_members AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_new_members

),

payment_method AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.PAYMENT_METHOD

),

subscription_collection_attempt AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT

),

subscription_payment AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT

),

subscribers AS (
    -- monthly subscribers, starting from Sept 2020
    -- one user can only have one bill in a billing cycle
    SELECT
        b.billing_cycle,
        b.user_id,
        b.id AS bill_id,
        b.due_date AS bill_due_date
    FROM subscription_billing b
    INNER JOIN user u ON u.id = b.user_id
    LEFT JOIN one_dave_new_members od ON u.id = od.user_id -- subscribers must be One Dave members
    WHERE b.due_date >= Date_trunc('month', CURRENT_DATE() - 365)
        AND b.amount > 0
        AND b._fivetran_deleted = false
        AND u._fivetran_deleted = false

),

attempts AS (
    -- attempts which reached payment processor
    SELECT
        created,
        subscription_billing_id AS bill_id,
        subscription_payment_id AS payment_id
    FROM subscription_collection_attempt
    WHERE created >= '2022-01-01'
),

attempt_group as (
    select
        bill_id,
        payment_id,
        coalesce(count(*), 0) as attempt_cnt
    from attempts
    group by 1, 2

),

payments AS (
    -- payment succeeded
    SELECT
        id AS payment_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS payment_pt_ts,
        DATE(payment_pt_ts) AS payment_pt_dt,
        user_id,
        amount
    FROM subscription_payment
    WHERE payment_pt_dt >= '2022-01-01'
        AND status in ('COMPLETED') -- check if pending should be included
        AND _FIVETRAN_DELETED = false
),

sub1 AS (
    -- join all the CTEs above to get a summary view of bills
    SELECT
        s.billing_cycle,
        s.user_id,
        s.bill_id as subscription_charge_id,
        p.payment_pt_ts as payment_pt,
        s.bill_due_date as term_started
    FROM subscribers s
    JOIN attempt_group a ON s.bill_id = a.bill_id
    LEFT JOIN payments p ON a.payment_id = p.payment_id
    LEFT JOIN sandbox.dev_yyang.legacy_refund refund
    on s.billing_cycle = refund.legacy_billing_cycle and s.user_id = refund.user_id
    where refund.user_id is NULL and p.amount > 0


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

subscriber_new AS (
    SELECT
        sub.USER_ID,
        sub.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id,
        od.event_ds as reg_date,
        TO_VARCHAR(od.EVENT_DS, 'YYYY-MM') as reg_month
    FROM sub_new sub
    JOIN tier_new tier on sub.tier_id = tier.id
    LEFT JOIN one_dave_new_members od on sub.user_id = od.user_id
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
        sc_charge.unit_cost
    FROM subscriber_new subscriber
    LEFT JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    where sc_charge._DELETED = FALSE
    and sc_charge.unit_cost > 0
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
        date(min(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created))) as first_attempt_date,
        date(max(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created))) as last_attempt_date,
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
        iff(ag.subscription_charge_id is not null, 1, 0) as attempted_to_collect,
        iff(s.collect_status= 'collected', 1, 0) as is_collect_succeeded,
        ag.first_attempt_date,
        ag.last_attempt_date,
        case when s.collect_status = 'collected' then ag.last_attempt_date else null end as payment_pt_date,
        coalesce(ag.attempt_cnt, 0) as attempt_cnt
    from sub_charge_new s
    left join attempt_group_new as ag on s.subscription_charge_id = ag.subscription_charge_id
   WHERE is_collect_succeeded = 1

),

success as (
    select
        billing_cycle,
        user_id,
        subscription_charge_id,
        term_started
    from sub2
    union
    select
        billing_cycle,
        user_id,
        to_varchar(subscription_charge_id) as subscription_charge_id,
        term_started
    from sub1


)
--select * from success where user_id = '01HCBGSRHCECXZJHMEYAHJCBK2' or subscription_charge_id = '01HCBGSRHCECXZJHMEYAHJCBK2' or term_started = '01HCBGSRHCECXZJHMEYAHJCBK2'
select
billing_cycle,
term_started,
user_id,
subscription_charge_id,
lag(term_started) over (partition by user_id order by term_started) as last_term_start
---lead(term_started) over (partition by user_id order by term_started) as next_term_start
from success

--------

with march as (
    select user_id from sandbox.dev_yyang.sub1_tmp where billing_cycle = '2023-03' or billing_cycle = '2023-03-01'
),
april as (
    select user_id from sandbox.dev_yyang.sub1_tmp where billing_cycle = '2023-04' or billing_cycle = '2023-04-01'
),
missed as (
    select
    march.user_id
    from march
    left join april
  on march.user_id = april.user_id
  where april.user_id is null

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
    where _FIVETRAN_DELETED = TRUE
    UNION
    SELECT DISTINCT ID AS user_id FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER
    where _FIVETRAN_DELETED = TRUE
),
pd as (
    select * from paused_update
    union
    select distinct user_id from deleted_user
)
select
missed.user_id
from missed
left join pd
on missed.user_id = pd.user_id
where pd.user_id is null