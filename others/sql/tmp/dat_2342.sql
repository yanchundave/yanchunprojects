create table sandbox.dev_yyang.creditlimit as
with users as
(
    select
        user_id,
        bill_id,
        bill_due_date,
        payment_method,
        is_collect_succeeded,
        billing_cycle
    from  ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date = DATE('2023-08-04')


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
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) >= '2023-08-03 16:00:00.000'
    and CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) < '2023-08-06 00:00:01.000'
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
      a.payment_method,
      a.is_collect_succeeded,
      a.billing_cycle,
      b.attempt_id,
      b.created_pst,
      b.chargeType,
      b.status,
      b.external_processor,
      b.payment_id,
      b.processor
    from users a
    left join payments b
    on a.bill_id = b.subscription_billing_id
)
select * from final

-----subscriber
with success_billing as (
    select
    *,
    row_number() over (partition by bill_id order by created_pst desc) as rownumber
  from sandbox.dev_yyang.creditlimit
  where attempt_id is not null
  qualify rownumber = 1
)
select date(created_pst) as attempt_date, processor, count(distinct user_id) as users
from success_billing
where is_collect_succeeded = 1
group by 1, 2

---Attempt
with success_billing as (
    select
    *
  from sandbox.dev_yyang.creditlimit
  where attempt_id is not null
)
select date(created_pst), processor, count(distinct attempt_id) from success_billing
group by 1, 2
order by 1, 2

------July

create table sandbox.dev_yyang.creditlimit_july as
with users as
(
    select
        user_id,
        bill_id,
        bill_due_date,
        payment_method,
        is_collect_succeeded,
        billing_cycle
    from  ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS
    where bill_due_date = DATE('2023-07-07')


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
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) >= '2023-07-07 00:00:01.000'
    and CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) < '2023-07-09 00:00:01.000'
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
      a.payment_method,
      a.is_collect_succeeded,
      a.billing_cycle,
      b.attempt_id,
      b.created_pst,
      b.chargeType,
      b.status,
      b.external_processor,
      b.payment_id,
      b.processor
    from users a
    left join payments b
    on a.bill_id = b.subscription_billing_id
)
select * from final