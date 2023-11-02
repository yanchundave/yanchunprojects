sub2 as (
    select
      date(date_combine.start_date) as start_date,
      date(date_combine.end_date) as end_date,
      date_part('day', date(date_combine.end_date)) as daynum,
      sum(coalesce(final.attempt_cnt, 0)) as attempt_total_cnt,
      count(distinct final.user_id) as attempted_user,
      sum(final.if_succeed) as total_success,
      sum(final.unit_cost * final.if_succeed) as collect_amount
    from date_combine
    left join  final_new final
    on final.monthnum = date_combine.start_date and final.attempt_date <= date_combine.end_date
    group by 1, 2, 3
    order by start_date, end_date

),

combined as (
    select
        coalesce(sub1.start_date, sub2.start_date) as start_date,
        coalesce(sub1.end_date, sub2.end_date) as end_date,
        coalesce(sub1.daynum, sub2.daynum) as daynum,
        coalesce(sub1.attempt_total_cnt, 0) + coalesce(sub2.attempt_total_cnt, 0) as attempt_total_cnt,
        coalesce(sub1.attempted_user, 0) + coalesce(sub2.attempted_user, 0) as attempted_user,
        coalesce(sub1.total_success, 0) + coalesce(sub2.total_success, 0) as total_success,
        coalesce(sub1.collect_amount, 0) + coalesce(sub2.collect_amount, 0) as collect_amount,
        'combine' as label
    from sub1
    full join sub2
    on sub1.start_date = sub2.start_date
     and sub1.end_date = sub2.end_date
     and sub1.daynum = sub2.daynum
    union all
    select
        start_date,
        end_date,
        daynum,
        coalesce(attempt_total_cnt, 0) as attempt_total_cnt,
        coalesce(attempted_user, 0) as attempted_user,
        coalesce(total_success, 0) as total_success,
        coalesce(collect_amount, 0) as collect_amount,
        'sub1.0' as label
    from sub1
    union all
    select
        start_date,
        end_date,
        daynum,
        coalesce(attempt_total_cnt, 0) as attempt_total_cnt,
        coalesce(attempted_user, 0) as attempted_user,
        coalesce(total_success, 0) as total_success,
        coalesce(collect_amount, 0) as collect_amount,
        'sub2.0' as label
    from sub2
)
select * from combined
--where label = '{{ selected_label_for_pacing_metrics }}'
where label = 'sub2.0'

{% form %}

selected_label_for_pacing_metrics:
  type: select
  default: sub2.0
  options: [combine, sub1.0, sub2.0]

{% endform %}

--------

with bank as
(select
      date_trunc(month, bank_activity_ds_pst) AS reporting_month,
       USER_ID,
       'bank' as product
from ANALYTIC_DB.DBT_metrics.bank_active_users_old_definition
where bank_activity_ds_pst >= TO_DATE('2019-01-01')
),

o2 as
(select
        date_trunc(month, transacting_ds_pst) AS reporting_month,
        USER_ID,
        'credit' as product
from ANALYTIC_DB.DBT_METRICS.OVERDRAFT_ACTIVE_USERS
),

advance as
(select
       date_trunc(month, transacting_ds_pst) AS reporting_month,
       USER_ID,
       'credit' as product
from ANALYTIC_DB.DBT_metrics.advance_active_users
where transacting_ds_pst >= TO_DATE('2019-01-01')
),

dateadd('month', -10, current_date())