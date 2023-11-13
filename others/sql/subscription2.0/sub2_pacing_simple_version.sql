create or replace table sandbox.dev_yyang.sub_pacing as
WITH subscription_billing AS (

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
        b.due_date AS bill_due_date,
        od.event_ds AS reg_date, -- date when users become one dave member
        TO_VARCHAR(od.EVENT_DS, 'YYYY-MM') as reg_month,
        CASE WHEN reg_month < b.billing_cycle THEN 1 ELSE 0 END AS is_existing_subscriber
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
        subscription_payment_id AS payment_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS attempt_pt_ts,
        DATE(attempt_pt_ts) AS attempt_pt_dt
    FROM subscription_collection_attempt
    WHERE attempt_pt_dt >= '2020-01-01'
),

attempt_group as (
    select
        bill_id,
        payment_id,
        date(attempt_pt_ts) as attempt_date,
        coalesce(count(*), 0) as attempt_cnt
    from attempts
    group by 1, 2, 3

),

payments AS (
    -- payment succeeded
    SELECT
        id AS payment_id,
        created,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS payment_pt_ts,
        DATE(payment_pt_ts) AS payment_pt_dt,
        user_id,
        amount
    FROM subscription_payment
    WHERE payment_pt_dt >= '2020-01-01'
        AND status in ('COMPLETED','PENDING') -- check if pending should be included
        AND _FIVETRAN_DELETED = false
),

final AS (
    -- join all the CTEs above to get a summary view of bills
    SELECT
        s.billing_cycle,
        s.user_id,
        s.bill_id as subscription_charge_id,
        a.attempt_date,
        DATE_TRUNC('month', a.attempt_date) as monthnum,
        a.attempt_cnt,
        coalesce(p.amount, 0) as unit_cost,
        iff(p.amount > 0, 1, 0) as if_succeed,
        'N' as collect_status,
        'sub1.0' as label
    FROM subscribers s
    JOIN attempt_group a ON s.bill_id = a.bill_id
    LEFT JOIN payments p ON a.payment_id = p.payment_id and a.attempt_date = p.payment_pt_dt
    LEFT JOIN sandbox.dev_yyang.legacy_refund refund
    on s.billing_cycle = refund.legacy_billing_cycle and s.user_id = refund.user_id
    where refund.user_id is NULL

),
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
start_month as
(
  select distinct date_trunc('month', ref_date) as start_date from cte_attempt_date
),
date_combine as (
    select
        a.start_date,
        b.ref_date as end_date
    from start_month a
    join cte_attempt_date b
    on a.start_date = date_trunc('month', b.ref_date)

),
sub1 as (
    select
        date(date_combine.start_date) as start_date,
        date(date_combine.end_date) as end_date,
        date_part('day', date(date_combine.end_date)) as daynum,
        sum(final.attempt_cnt) as attempt_total_cnt,
        count(distinct final.user_id) as attempted_user,
        sum(final.if_succeed) as total_success,
        sum(final.unit_cost * final.if_succeed) as collect_amount

    from date_combine
    left join  final
    on final.monthnum = date_combine.start_date and final.attempt_date <= date_combine.end_date
    group by 1, 2, 3
    order by start_date, end_date
 ),

-- sub2.0
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
subscriber_new AS (
    SELECT
        sub.USER_ID,
        sub.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id
    FROM sub_new sub
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
        sc_charge.unit_cost
    FROM subscriber_new subscriber
    JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    qualify row_number() over (partition by billing_cycle, user_id order by term_started) = 1
),
sub_attempts_new AS (
    SELECT
        a.id as subscription_attempt_id,
        s.subscription_charge_id,
        a.created,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED)) AS attempt_date,
        DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED))) as monthnum,
        s.billing_cycle,
        s.user_id
    FROM attempts_new a
    JOIN sub_charge_new s on a.subscription_charge_id = s.subscription_charge_id
    order by monthnum, attempt_date, created
),
attempt_group_new as
(
    select
        billing_cycle,
        user_id,
        subscription_charge_id,
        attempt_date,
        monthnum,
        count(distinct subscription_attempt_id) as attempt_cnt
    from sub_attempts_new
    group by 1, 2, 3, 4, 5
),
last_attempt_new as (
    select
        subscription_charge_id,
        date(max(DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CREATED)))) as last_attempt_date
    from attempts_new
    group by 1
),
success_sub_new as (
    select
        sc.billing_cycle,
        sc.term_started,
        sc.user_id,
        sc.subscription_charge_id,
        sc.collect_status,
        sc.unit_cost,
        la.last_attempt_date
    from sub_charge_new sc
    join last_attempt_new la
    on sc.subscription_charge_id = la.subscription_charge_id
    where sc.unit_cost > 0
),
final_new as (
    select
        a.billing_cycle,
        a.user_id,
        a.subscription_charge_id,
        a.attempt_date,
        a.monthnum,
        a.attempt_cnt,
        s.unit_cost / 100 as unit_cost,
        case
        when s.collect_status = 'collected' then 1
        else 0
        end as if_succeed,
        s.collect_status
    from attempt_group_new a
    left join success_sub_new s
    on a.subscription_charge_id = s.subscription_charge_id
    and a.attempt_date = s.last_attempt_date
) select * from final_new