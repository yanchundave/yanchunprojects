--sandbox.dev_yyang.system_churned
WITH
last_bill as
(
    SELECT
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', term_started)) as term_started,
        dayofmonth(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', term_started)) as day_of_month,
        user_id
    FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
    qualify row_number() over (partition by user_id order by term_started desc) = 1

),

RankedSubscriptions AS (
  SELECT
    ID,
    USER_ID,
    STARTED,
    CANCELED,
    ENDED,
    ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY STARTED DESC) AS rn
  FROM
    dave.subscription.subscription
  WHERE _deleted = FALSE
),
users as (
SELECT DISTINCT
  rs.*
FROM
  RankedSubscriptions rs
WHERE
  rs.rn = 1
  AND rs.ENDED IS NOT NULL
  AND rs.CANCELED IS NOT NULL
  AND date_trunc('DAY', canceled) = date_trunc('DAY', ended)
),
paused_resumed as (
  select user_id,
   event_type,
   CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) as event_time,
   case
   when event_type like ('subscriptions pause membership button pressed') then 'paused'
   when event_type like ('subscriptions resume membership button pressed') then 'resumed'
   else 'None'
   end as pause_or_not
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time) >= date('2022-10-27')
    and event_type like '%subscriptions pause membership button pressed%' or
    event_type like '%subscriptions resume membership button pressed%'
    qualify row_number() over (partition by user_id order by event_time desc) = 1
),
tmp as (
  select
  a.id,
  a.user_id,
  a.started,
  a.canceled,
  a.ended,
  b.event_time,
  b.pause_or_not,
  c.term_started as last_term_start,
  c.day_of_month as last_day_of_month,
  datediff('second', canceled, ended) as seconddiff,
  datediff('day', event_time, ended) as daydiff_event
  from users a
  left join paused_resumed b
  on a.user_id = b.user_id
  left join last_bill c
  on a.user_id = c.user_id
  )
  select * from tmp