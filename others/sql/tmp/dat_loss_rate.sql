with
recursive_attempt_date as
(
    select
        1 as start_date,
        31 as end_date
),
cte_attempt_date(ref_date) as
(
    select
        start_date as ref_date
        from recursive_attempt_date
        union all
        select
        ref_date + 1 as ref_date
        from cte_attempt_date
        cross join
        recursive_attempt_date
        where ref_date < end_date
),
month_num as
(
  select ref_date from cte_attempt_date
),
subs as (
  select *, date_trunc('week', term_started) as weekly_number,
  datediff('day', term_started, coalesce(last_attempt_date, current_date())) as datediff,
  UPPER(dayname(TERM_STARTED)) as dayofweek
  from analytic_db.dbt_marts.fct_subscription2_collections where term_started >= date('2023-10-01')
  ),
  weekly as (
    select
    weekly_number,
    dayofweek,
    count(distinct subscription_charge_id) as sub_cnt
    from subs
    group by 1, 2
  ),
  daily as (
    select
        weekly_number,
        dayofweek,
        datediff,
        count(distinct subscription_charge_id) as sub_success
    from subs
    where collect_status = 'collected'
    group by 1, 2 ,3
  ),
  accumulate_sub as (
    select
    a.ref_date,
    b.weekly_number,
    b.dayofweek,
    sum(sub_success) as success_cnt
    from month_num a
    left join daily b
    on a.ref_date >= b.datediff
    group by 1, 2, 3
  ),
  final as (
    select
    b.weekly_number,
    b.dayofweek,
    a.ref_date,
    b.sub_cnt,
    a.success_cnt
    from accumulate_sub a
    join weekly b
    on a.weekly_number = b.weekly_number and a.dayofweek = b.dayofweek
  )
  select * from final