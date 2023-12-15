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
        date_trunc('month', s.bill_due_date) as billing_cycle,
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

sub2 as (

    select
        billing_cycle,
        user_id,
        subscription_charge_id,
        term_started

    from analytic_db.dbt_marts.fct_subscription2_collections
   where collect_status='collected' and UNIT_COST > 0

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


),
earliest as (
    select user_id, min(billing_cycle) as start_date from success group by user_id
),
combined as (

    select
        a.user_id,
        a.start_date,
        datediff('month', a.start_date, b.billing_cycle) as monthdiff
    from earliest a
    left join success b
    on a.user_id = b.user_id
)
select * from combined