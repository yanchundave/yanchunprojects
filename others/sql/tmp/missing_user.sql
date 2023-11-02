create or replace table sandbox.dev_yyang.users_missing as
with octbill as (
    select * from sandbox.dev_yyang.legacy_oct20 where billing_cycle='2023-10'
)
select
a.*, b.bill_due_date as bill_due_date_in_oct, b.is_collect_succeeded as oct_collected, b.payment_pt_dt as oct_payment_dt, c.subscription_fee
from sandbox.dev_yyang.users_inlegacy a
left join octbill b
on a.user_id = b.user_id
left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER c
on a.user_id = c.id


------

with octbill as (
    select * from sandbox.dev_yyang.legacy_oct20 where billing_cycle='2023-10'
)
select
a.*, b.bill_due_date as bill_due_date_in_oct, b.is_collect_succeeded as oct_collected, b.payment_pt_dt as oct_payment_dt, c.subscription_fee
from sandbox.dev_yyang.users_inlegacy a
join octbill b
on a.user_id = b.user_id
left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER c
on a.user_id = c.id