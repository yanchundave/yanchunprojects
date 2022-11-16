With advance_payment_webhook_base as (
    select advance_id,
    id as payment_id,
    amount as payment_amount,
    date(created) as payment_date,
    WEBHOOK_DATA
    from APPLICATION_DB.TRANSACTIONS_DAVE.PAYMENT
    where external_processor = 'SYNAPSEPAY'
      AND STATUS = 'RETURNED'
      AND EXTERNAL_ID IS NOT NULL
      AND WEBHOOK_DATA <> '[]'
)
-- A payment_id can also have multiple status_code
-- one of the payment needed to be returned within repayment period to be R20 attributed (if R20 happen after, it doesn't count)
, advance_payment_status_base as (
    select
      a.payment_id,
      a.payment_amount,
      a.payment_date,
      b.VALUE :recent_status: date['$date'] as date_ts,
      b.VALUE :recent_status :code AS status_code
  from advance_payment_webhook_base a,
        TABLE(flatten(a.WEBHOOK_DATA)) b
  where not IS_NULL_VALUE(status_code) -- variant null can't be handled by "is not"
  -- if status code is a variant null, that means the advance payment can't be R01 or R20 error
  -- even if the payment failed or advance loss
  qualify row_number() over (partition by a.payment_id order by date_ts desc) = 1
)