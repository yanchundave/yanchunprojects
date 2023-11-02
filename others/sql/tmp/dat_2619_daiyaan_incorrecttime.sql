--step 1: generate legacy_base
create or replace table sandbox.dev_yyang.legacy_users as
WITH subscription_billing AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_BILLING

),

user AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER

),

one_dave_new_members AS (

    SELECT * FROM ANALYTIC_DB.DBT_metrics.one_dave_new_members

),

bank_connection AS (

    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION

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
    WHERE b.billing_cycle >= '2023-01'
        AND b.amount > 0
        AND b._fivetran_deleted = false
        AND u._fivetran_deleted = false
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
    FROM payment_method
    WHERE INVALID IS NULL
          AND DELETED IS NULL
          AND EXPIRATION > CURRENT_DATE()
          AND _FIVETRAN_DELETED = false
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

payments AS (
    -- payment succeeded
    SELECT
        id AS payment_id,
        created,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS payment_pt_ts,
        DATE(payment_pt_ts) AS payment_pt_dt,
        external_processor,
        CASE external_processor
            WHEN 'TABAPAY' THEN 'Debit'
            WHEN 'SYNAPSEPAY' THEN 'ACH'
            ELSE 'undetermined'
        END AS payment_method,
        user_id,
        amount
    FROM subscription_payment
    WHERE payment_pt_dt >= '2020-01-01'
        AND status in ('COMPLETED') -- check if pending should be included
        AND _FIVETRAN_DELETED = false
),

bill_agg AS (
    -- join all the CTEs above to get a summary view of bills
    SELECT
        s.billing_cycle,
        s.user_id,
        s.bill_id,
        s.bill_due_date,
        s.reg_date,
        s.reg_month,
        s.is_existing_subscriber,
        u.user_bc_healthiness_score AS bc_healthiness_score,
        IFF(d.user_id IS NOT NULL, 1, 0) AS has_debit_card,

        MAX(IFF(a.bill_id IS NOT NULL, 1, 0)) AS attempted_to_collect,
        MAX(IFF(p.payment_id IS NOT NULL, 1, 0)) AS is_collect_succeeded,

        -- update on 11/19/21: add first attempt and payment date
        MIN(a.attempt_pt_dt) AS first_attempt_pt_dt,
        MIN(p.payment_pt_dt) AS payment_pt_dt,

        -- update on 12/1/2021: add attempt counts
        COALESCE(COUNT(a.bill_id), 0) AS attempt_cnt,

        -- update on 12/10/2021: add payment method
        MAX(p.payment_method) AS payment_method
    FROM subscribers s
    LEFT JOIN user_bank_connection_healthiness u ON s.user_id = u.user_id
    LEFT JOIN debit_card_user d ON s.user_id = d.user_id
    LEFT JOIN attempts a ON s.bill_id = a.bill_id
    LEFT JOIN payments p ON a.payment_id = p.payment_id
    GROUP BY 1,2,3,4,5,6,7,8,9
)

-- join audit log to get first collection trigger info

SELECT
    b.billing_cycle,
    b.user_id,
    b.bill_id,
    b.bill_due_date,
    b.reg_date,
    b.reg_month,
    b.is_existing_subscriber,
    b.bc_healthiness_score,
    b.has_debit_card,
    b.attempted_to_collect,
    b.is_collect_succeeded,
    b.first_attempt_pt_dt,
    b.payment_pt_dt,
    b.attempt_cnt,
    b.payment_method
FROM bill_agg b

-- step 2: generate sub2_users
create or replace table sandbox.dev_yyang.sub2_users as
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
        payment.kind as payment_method,
        sc_charge.charged_payment_method_id,
        sc_charge.reference_id
    FROM subscriber_new subscriber
    LEFT JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
    where sc_charge._DELETED = FALSE
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
        s.payment_method,
        s.unit_cost,
        s.collect_status,
        s.charged_payment_method_id,
        s.reference_id
    from sub_charge_new s
    left join attempt_group_new as ag on s.subscription_charge_id = ag.subscription_charge_id
    left join user_bank_connection_healthiness u on s.user_id = u.user_id
    left join debit_card_user d on s.user_id = d.user_id


)
select * from sub2

--- step 3: generate legacy_sub2

create or replace table sandbox.dev_yyang.legacy_sub2 as
with
sub2 as (
  select * from sandbox.dev_yyang.sub2_users
),
legacy as (
    select * from sandbox.dev_yyang.legacy_users
    where bill_due_date >= date('2023-06-01')
)
select
legacy.user_id,
legacy.bill_id as legacy_bill_id,
legacy.billing_cycle as legacy_billing_cycle,
legacy.bill_due_date as legacy_bill_due_date,
legacy.is_collect_succeeded as legacy_collected,
legacy.payment_pt_dt as legacy_payment_pt,
sub2.term_started as sub2_term_started,
sub2.collect_status as sub2_collect_status,
sub2.subscription_charge_id as sub2_subscription_charge_id,
sub2.payment_pt_date
from legacy
join  sub2
on legacy.user_id = sub2.user_id
and TO_VARCHAR(legacy.billing_cycle) = sub2.billing_cycle
where sub2.unit_cost > 0
and legacy_collected = 1 and sub2_collect_status='collected'

-- step 4: generate legacy_refunding

create or replace table sandbox.dev_yyang.legacy_refund as
with
subscription_collection_attempt AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT

),

subscription_payment AS (

    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT

),
legacy as (

    select user_id, legacy_bill_id, legacy_billing_cycle
    from sandbox.dev_yyang.legacy_sub2
    where legacy_collected = 1 and sub2_collect_status = 'collected'
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
payments AS (
    -- payment succeeded
    SELECT
        id AS payment_id,
        created,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) AS payment_pt_ts,
        DATE(payment_pt_ts) AS payment_pt_dt,
        external_processor,
        CASE external_processor
            WHEN 'TABAPAY' THEN 'Debit'
            WHEN 'SYNAPSEPAY' THEN 'ACH'
            ELSE 'undetermined'
        END AS payment_method,
        user_id,
        bank_account_id,
        payment_method_id
    FROM subscription_payment
    WHERE payment_pt_dt >= '2020-01-01'
        AND status in ('COMPLETED') -- check if pending should be included
        AND _FIVETRAN_DELETED = false
),
legacy_payment as (
    select
    legacy.user_id,
    legacy.legacy_bill_id,
    legacy.legacy_billing_cycle,
    payments.payment_method,
    payments.payment_pt_dt,
    payments.payment_id,
    ---logic from steven
    case
        when payments.payment_method_id is not null then CONCAT('DEBIT:', payment_method_id)
        else
            case
            when payments.bank_account_id is not null then CONCAT('BANK:', bank_account_id)
            else NULL
            end
        end AS paymentMethodUniversalID
    from legacy
    join attempts
    on legacy.legacy_bill_id = attempts.bill_id
    join payments
    on attempts.payment_id = payments.payment_id
     qualify
            row_number() over (
                partition by legacy.user_id, legacy.legacy_bill_id order by payments.payment_pt_ts desc
            )
            = 1
)
select
user_id,
legacy_bill_id,
payment_id,
payment_method,
paymentmethoduniversalid,
legacy_billing_cycle
from legacy_payment

--- step 5: generate sub2_refunding

create or replace table sandbox.dev_yyang.sub2_refund as
with sub_charge_new AS (
    SELECT *
    from sandbox.dev_yyang.sub2_users
    where collect_status = 'collected'
    and term_started >= date('2023-06-01')

)
select
billing_cycle,
user_id,
subscription_charge_id,
reference_id,
charged_payment_method_id as paymentMethodUniversalID,
payment_method
from sub_charge_new
qualify
    row_number() over (partition by billing_cycle, user_id order by term_started ) > 1



----Daiyaan required doc

create or replace table sandbox.dev_yyang.user_charged_incorrect as
with tmp as (
  select user_id, payment_pt_dt
  from sandbox.dev_yyang.legacy_refund
  union all
  select user_id, payment_pt_date as payment_pt_dt
  from sandbox.dev_yyang.sub2_refund
),
tmp1 as (
  select
    user_id,
    payment_pt_dt,
    row_number() over (partition by user_id order by payment_pt_dt) as rownumber
  from tmp
),
numberone as
(
    select user_id, payment_pt_dt as first_dt
    from tmp1
    where rownumber = 1

),
second as
(
    select user_id, payment_pt_dt as second_dt
    from tmp1
    where rownumber = 2

),
third as
(
    select user_id, payment_pt_dt as third_dt
    from tmp1
    where rownumber = 3

),
fourth as
(
    select user_id, payment_pt_dt as fourth_dt
    from tmp1
    where rownumber = 4

)
select
  a.user_id,
  a.first_dt,
  b.second_dt,
  c.third_dt,
  d.fourth_dt
from numberone a
left join second b on a.user_id = b.user_id
left join third c on a.user_id = c.user_id
left join fourth d on a.user_id = d.user_id