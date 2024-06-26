------Total subscribers  1352391

WITH users as
(
    select user_id, bill_id, bill_due_date, is_existing_subscriber, bc_healthiness_score, has_debit_card,
    attempted_to_collect, is_collect_succeeded, payment_method,
    DATEADD(MONTH, -1, DATE_TRUNC('Month', bill_due_date)) as previous_bill_cycle
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date >= DATE('2023-06-26') AND BILL_DUE_DATE <= DATE('2023-07-10') AND BC_HEALTHINESS_SCORE=1
),
doubleusers as
(
    select user_id, count(distinct bill_id)
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where
        (billing_cycle = '2023-06' or billing_cycle = '2023-07')
        and bill_due_date >= DATE('2023-06-26')
        AND BILL_DUE_DATE <= DATE('2023-07-10')
    group by 1
    having count(*) > 1
),
userupdate as
(
    select
        users.*
    from users
    left join doubleusers
    on users.user_id = doubleusers.user_id
    where doubleusers.user_id is null
),
previoususer as
(
    select distinct user_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    where created >= date('2023-03-01') and created <= date('2023-06-26')
),
tmp as (
    select
        a.*
    from userupdate a
    join previoususer b
    on a.user_id = b.user_id
)
select count(distinct user_id) from tmp

-------Subscriber which did attempts, total attempts
--COUNT(DISTINCT USER_ID)	COUNT(ATTEMPT_ID)	SUM(ATTEMPTED_TO_COLLECT)	SUM(IS_COLLECT_SUCCEEDED)
--597359	628029	628029	584575

WITH users as
(
    select user_id, bill_id, bill_due_date, is_existing_subscriber, bc_healthiness_score, has_debit_card,
    attempted_to_collect, is_collect_succeeded, payment_method,
    DATEADD(MONTH, -1, DATE_TRUNC('Month', bill_due_date)) as previous_bill_cycle
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date >= DATE('2023-06-26') AND BILL_DUE_DATE <= DATE('2023-07-10') AND BC_HEALTHINESS_SCORE=1
),
doubleusers as
(
    select user_id, count(distinct bill_id)
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where
        (billing_cycle = '2023-06' or billing_cycle = '2023-07')
        and bill_due_date >= DATE('2023-06-26')
        AND BILL_DUE_DATE <= DATE('2023-07-10')
    group by 1
    having count(*) > 1
),
userupdate as
(
    select
        users.*
    from users
    left join doubleusers
    on users.user_id = doubleusers.user_id
    where doubleusers.user_id is null
),
previoususer as
(
    select distinct user_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    where created >= date('2023-03-01') and created <= date('2023-06-26')
),
tmp as (
    select
        a.*
    from userupdate a
    join previoususer b
    on a.user_id = b.user_id
),
attempts as
(
    select
        id,
        created,
        subscription_billing_id,
        parse_json(extra):chargeType as chargeType
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    where created >= date('2023-06-26') and created <= date('2023-07-10')

),
final as (

    select
        tmp.*,
        attempts.id as attempt_id
    from tmp
    join attempts
    on tmp.bill_id = attempts.subscription_billing_id
)
select count(distinct user_id), count(attempt_id), sum(attempted_to_collect), sum(is_collect_succeeded) from final
----------------
---------- success and return

--STATUS	COUNT(DISTINCT USER_ID)	COUNT(PAYMENT_ID)	SUM(IS_COLLECT_SUCCEEDED)
--COMPLETED	549384	549424	549424
--RETURNED	18017	18178	8357
--CANCELED	34545	48439	14936

WITH users as
(
    select user_id, bill_id, bill_due_date, is_existing_subscriber, bc_healthiness_score, has_debit_card,
    attempted_to_collect, is_collect_succeeded, payment_method,
    DATEADD(MONTH, -1, DATE_TRUNC('Month', bill_due_date)) as previous_bill_cycle
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date >= DATE('2023-06-26') AND BILL_DUE_DATE <= DATE('2023-07-10') AND BC_HEALTHINESS_SCORE=1
),
doubleusers as
(
    select user_id, count(distinct bill_id)
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where
        (billing_cycle = '2023-06' or billing_cycle = '2023-07')
        and bill_due_date >= DATE('2023-06-26')
        AND BILL_DUE_DATE <= DATE('2023-07-10')
    group by 1
    having count(*) > 1
),
userupdate as
(
    select
        users.*
    from users
    left join doubleusers
    on users.user_id = doubleusers.user_id
    where doubleusers.user_id is null
),
previoususer as
(
    select distinct user_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    where created >= date('2023-03-01') and created <= date('2023-06-26')
),
tmp as (
    select
        a.*
    from userupdate a
    join previoususer b
    on a.user_id = b.user_id
),
attempts as
(
    select
    id, created,
    subscription_billing_id,
    parse_json(extra):chargeType as chargeType,
    subscription_payment_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    where created >= date('2023-06-26') and created <= date('2023-07-10')

),
user_attempts as (

    select
        tmp.*,
        attempts.id as attempt_id,
        attempts.subscription_payment_id as payment_id
    from tmp
    join attempts
    on tmp.bill_id = attempts.subscription_billing_id
),
user_payments as (

    select
        a.*,
        b.status,
        b.external_processor

    from user_attempts a
    join APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT b
    on a.payment_id = b.id
)
select status, count(distinct user_id), count(payment_id), sum(is_collect_succeeded) from user_payments group by 1

----------
--ACH ATTEMPT
--STATUS	PROCESSOR	COUNT(ATTEMPT_ID)	PAYMENT_COUNT
--CANCELED	DEBIT	18623	18623
--RETURNED	ACH	18178	18178
--	DEBIT	19	0
--COMPLETED	DEBIT	114893	114893
----COMPLETED	ACH	434531	434531
----CANCELED	ACH	29802	29802
	ACH	11969	0
--CANCELED	other	14	14

WITH users as
(
    select user_id, bill_id, bill_due_date, is_existing_subscriber, bc_healthiness_score, has_debit_card,
    attempted_to_collect, is_collect_succeeded, payment_method,
    DATEADD(MONTH, -1, DATE_TRUNC('Month', bill_due_date)) as previous_bill_cycle
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date >= DATE('2023-06-26') AND BILL_DUE_DATE <= DATE('2023-07-10') AND BC_HEALTHINESS_SCORE=1
),
doubleusers as
(
    select user_id, count(distinct bill_id)
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where
        (billing_cycle = '2023-06' or billing_cycle = '2023-07')
        and bill_due_date >= DATE('2023-06-26')
        AND BILL_DUE_DATE <= DATE('2023-07-10')
    group by 1
    having count(*) > 1
),
userupdate as
(
    select
        users.*
    from users
    left join doubleusers
    on users.user_id = doubleusers.user_id
    where doubleusers.user_id is null
),
previoususer as
(
    select distinct user_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    where created >= date('2023-03-01') and created <= date('2023-06-26')
),
tmp as (
    select
        a.*
    from userupdate a
    join previoususer b
    on a.user_id = b.user_id
),
attempts as
(
    select
    id, created,
    subscription_billing_id,
    parse_json(extra):chargeType as chargeType,
    subscription_payment_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    where created >= date('2023-06-26') and created <= date('2023-07-10')

),
user_attempts as (

    select
        tmp.*,
        attempts.id as attempt_id,
        attempts.subscription_payment_id as payment_id,
        attempts.chargeType
    from tmp
    join attempts
    on tmp.bill_id = attempts.subscription_billing_id
),
user_payments as (

    select
        a.*,
        b.status,
        b.external_processor,
        b.id,
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

    from user_attempts a
    left join APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT b
    on a.payment_id = b.id
)
select status, processor, count(attempt_id), count(distinct id) as payment_count from user_payments group by 1, 2

-------SUCCESS CALCULATED
WITH users as
(
    select user_id, bill_id, bill_due_date, is_existing_subscriber, bc_healthiness_score, has_debit_card,
    attempted_to_collect, is_collect_succeeded, payment_method,
    DATEADD(MONTH, -1, DATE_TRUNC('Month', bill_due_date)) as previous_bill_cycle
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date >= DATE('2023-06-26') AND BILL_DUE_DATE <= DATE('2023-07-10') AND BC_HEALTHINESS_SCORE=1
),
doubleusers as
(
    select user_id, count(distinct bill_id)
    from ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where
        (billing_cycle = '2023-06' or billing_cycle = '2023-07')
        and bill_due_date >= DATE('2023-06-26')
        AND BILL_DUE_DATE <= DATE('2023-07-10')
    group by 1
    having count(*) > 1
),
userupdate as
(
    select
        users.*
    from users
    left join doubleusers
    on users.user_id = doubleusers.user_id
    where doubleusers.user_id is null
),
previoususer as
(
    select distinct user_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT
    where created >= date('2023-03-01') and created <= date('2023-06-26')
),
tmp as (
    select
        a.*
    from userupdate a
    join previoususer b
    on a.user_id = b.user_id
),
attempts as
(
    select
        id,
        created,
        subscription_billing_id,
        parse_json(extra):chargeType as chargeType
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    where created >= date('2023-06-26') and created <= date('2023-07-10')

),
final as (

    select
        tmp.*,
        attempts.id as attempt_id
    from tmp
    join attempts
    on tmp.bill_id = attempts.subscription_billing_id
)
select COUNT(DISTINCT user_id), sum(IS_COLLECT_SUCCEEDED) FROM FINAL
WHERE IS_COLLECT_SUCCEEDED = 1