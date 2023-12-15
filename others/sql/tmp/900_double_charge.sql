with overlapping_terms as (
select
sc.id,
sc.created,
sc.subscription_id,
DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', term_started)) as term_started,
DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', term_ended)) as term_ended,
lead(DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', term_started))) over (PARTITION BY sc.subscription_id order by term_started) as next_term_started
from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE sc
left join DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE_ATTEMPT sca on sca.subscription_charge_id = sc.id
where sc._DELETED = false and unit_cost > 1 and sca.completed_at is not null -- and sc.id = '01HDW8QFDVBM326SB952BG0ZNZ'
)
select
s.started, overlapping_terms.*
from overlapping_terms
join DAVE.SUBSCRIPTION.SUBSCRIPTION s on s.id = overlapping_terms.subscription_id
where next_term_started is not null and next_term_started < term_ended and term_started != next_term_started
order by term_started, s.id