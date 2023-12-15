---Laili required blind debit collection

with subscription as
(
  select ID, term_started from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
)
select
a.*,
c.term_started,
b.kind as payment_method
from dave.subscription.subscription_charge_attempt a
left join dave.subscription.payment_method_kind b
on a.payment_method_kind_id = b.id
left join subscription c
on a.SUBSCRIPTION_CHARGE_ID = c.id
where a.scheduled_for is not null
and b.kind = 'external_debit' and a.deleted is null
and external_account_balance_outcome in ('unchecked', 'unavailable') and a.CREATED >= date('2023-11-14')