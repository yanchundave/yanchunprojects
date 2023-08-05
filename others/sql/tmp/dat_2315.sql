--- New  version

-- Total subscriber
SELECT
  count(distinct sub.user_id)
FROM DAVE.subscription.subscription sub
INNER JOIN DAVE.subscription.tier tier
ON sub.tier_id = tier.id AND tier.code = 'one_dollar'
WHERE started >= date('2023-06-26') and started <= date('2023-07-10') and sub._DELETED = 'FALSE'

--- Total Attempts
with sub as
(
    select
        sub.id ,
        sub.user_id
    from DAVE.subscription.subscription sub
    inner join DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar'
    where
        started >= date('2023-06-26') and
        started <= date('2023-07-10') and
        sub._DELETED = 'FALSE'
),
sc as
(
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
scupdate as
(
    select
        sub.user_id,
        sub.id as subscription_id,
        sc.id as subscription_charge_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt
    from sub
    join sc on sub.id = sc.subscription_id
    --where term_started_pt >= Date('2023-06-26') and term_started_pt <= Date('2023-07-10')
),
attempts as
(
  SELECT
    subscription_charge_id,
    count(distinct id) as attempt_cnt

  FROM DAVE.SUBSCRIPTION.subscription_charge_attempt
  WHERE DELETED IS NULL
        AND _DELETED = false
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created)  >= Date('2023-06-26')
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created) <= Date('2023-07-10')
  group by 1
)
select
    sum(attempts.attempt_cnt),
    count(distinct user_id)
from scupdate
join attempts on scupdate.subscription_charge_id = attempts.subscription_charge_id

----------
--- success subscribers

with sub as
(
    select
        sub.id ,
        sub.user_id
    from DAVE.subscription.subscription sub
    inner join DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar'
    where
        started >= date('2023-06-26') and
        started <= date('2023-07-10') and
        sub._DELETED = 'FALSE'
),
sc as
(
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
scupdate as
(
    select
        sub.user_id,
        sub.id as subscription_id,
        sc.id as subscription_charge_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt,
        sc.subscription_charge_status_id
    from sub
    join sc on sub.id = sc.subscription_id
    where term_started_pt >= Date('2023-06-26') and term_started_pt <= Date('2023-07-10')
),
sc_status as (
    select * from DAVE.SUBSCRIPTION.subscription_charge_status
),
collection_status as (
    select
        scupdate.user_id,
        scupdate.subscription_id,
        scupdate.subscription_charge_id,
        sc_status.code as collection_status
    from scupdate
    join sc_status
    on scupdate.subscription_charge_status_id = sc_status.id
    where sc_status.code in ('collected', 'pending_collection')
),
last_attempt as (
  select
    subscription_charge_id,
    max(created) as latest_date
  from DAVE.SUBSCRIPTION.subscription_charge_attempt
  group by 1

),
success_attempt as (
  select
    a.*,
    b.latest_date,
    iff(b.latest_date >= date('2023-06-26') and b.latest_date <= date('2023-07-10'), 'in', 'out') as status
  from collection_status a
  join last_attempt b
  on a.subscription_charge_id = b.subscription_charge_id
)
select
    status,
    count(distinct user_id)
from success_attempt
group by 1

------ Success payments

with sub as
(
    select
        sub.id ,
        sub.user_id
    from DAVE.subscription.subscription sub
    inner join DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar'
    where
        started >= date('2023-06-26') and
        started <= date('2023-07-10') and
        sub._DELETED = 'FALSE'
),
sc as
(
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
scupdate as
(
    select
        sub.user_id,
        sub.id as subscription_id,
        sc.id as subscription_charge_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt,
        sc.subscription_charge_status_id,
        unit_cost
    from sub
    join sc on sub.id = sc.subscription_id
    --where term_started_pt >= Date('2023-06-20') and term_started_pt <= Date('2023-07-10')
),
sc_status as (
    select * from DAVE.SUBSCRIPTION.subscription_charge_status
),
collection_status as (
    select
        scupdate.user_id,
        scupdate.subscription_id,
        scupdate.subscription_charge_id,
        sc_status.code as collection_status,
        scupdate.unit_cost
    from scupdate
    join sc_status
    on scupdate.subscription_charge_status_id = sc_status.id
    where sc_status.code in ('collected', 'pending_collection')
),
attempts AS (
    SELECT
        SUBSCRIPTION_CHARGE_ID,
        MAX(created) as latest_date
    FROM DAVE.SUBSCRIPTION.subscription_charge_attempt
    group by 1
),
success_attempt as (
  select
      a.*,
      iff(b.latest_date >= date('2023-06-26') and b.latest_date <= date('2023-07-10'), 'in', 'out') as status
  from collection_status a
  join attempts b
  on a.subscription_charge_id = b.subscription_charge_id
  where a.collection_status = 'collected' or a.collection_status = 'pending_collection'
  and latest_date >= Date('2023-06-26') and latest_date<= Date('2023-07-10')
)
SELECT
status,
sum(coalesce(unit_cost, 0))/100 as revenue,
count(distinct user_id)
from success_attempt
group by 1



