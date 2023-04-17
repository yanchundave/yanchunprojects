DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll as
    select
        a.user_id,
        a.advance_id,
        a.daytime,
        b.daytime as last_payment_date,
        c.daytime as next_payment_date,
        datediff(day, b.daytime, a.daytime) as after_pay_diff,
        datediff(day, a.daytime, c.daytime) as before_pay_diff
    from DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_advance a
    left join DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_pay b
    on a.user_id = b.user_id
    and a.totalorder = b.totalorder
  left join DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_pay c
  on a.user_id = c.user_id
  and a.totalorder = c.totalorder - 1
--------

DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_advance;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_advance as
WITH userstart AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
payroll_info as
(
    select
        distinct
        d.user_id,
        c.payroll_date as daytime,
        1 as payrollorder,
        'pay' as typeclass
    from DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll
),
requests as
(
    select
        a.user_id,
        b.requested_ds_pst as daytime,
        0 as payrollorder,
        'advance' as typeclass
    from userstart a
    join ANALYTIC_DB.DBT_marts.requests b
),
combined as
(
    select *
    from (
    select * from payroll_info
    union all
    select * from requests
    )
    order by user_id, daytime
),
advance_pay as
(
    select
    user_id,
    advance_id,
    daytime,
    payrollorder,
    typeclass,
    sum(payrollorder) over (partition by user_id order by daytime asc between unbounded preceding and current row) as totalorder
    from combined
),
advance as
(
    select * from advance_pay where typeclass = 'advance'
),
pay as
(
    select * from advance_apy where typeclass = 'pay'
),
advance_pay_reset as
(
    select
        a.user_id,
        a.daytime,
        b.daytime as last_payment_date,
        c.daytime as next_pay_date
    from advance a
    join pay b
    on a.user_id = b.user_id
    and a.totalorder = b.totalorder
    join pay c
    on a.user_id = b.user_id
    and a.totalorder = b.totalorder - 1
)
select * from advance_pay_reset
-----------------------------------

DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_advance;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_advance as
WITH userstart AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
payroll_info as
(
    select
        distinct
        user_id,
        daytime,
        1 as payrollorder,
        'pay' as typeclass,
        '0' as advance_id
    from DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll
),
requests as
(
    select
        distinct
        a.user_id,
        b.disbursement_ds_pst as daytime,
        0 as payrollorder,
        'advance' as typeclass,
       b.advance_id
    from userstart a
    join ANALYTIC_DB.DBT_marts.disbursements b
    on a.user_id = b.user_id
    where b.product = 'Extra Cash' and b.disbursement_ds_pst > date('2022-12-15')
),
combined as
(
    select *
    from (
    select * from payroll_info
    union all
    select * from requests
    )
    order by user_id, daytime
),
advance_pay as
(
    select
      user_id,
      advance_id,
      daytime,
      payrollorder,
      typeclass,
      sum(payrollorder) over (partition by user_id order by daytime asc rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as totalorder
    from combined
),
advance as
(
    select * from advance_pay where typeclass = 'advance'
)
select * from advance

---------------- pay

DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_pay;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll_pay as
WITH userstart AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
payroll_info as
(
    select
        distinct
        user_id,
        daytime,
        1 as payrollorder,
        'pay' as typeclass,
        0 as advance_id
    from DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll
),
requests as
(
    select
        distinct
        a.user_id,
        b.disbursement_ds_pst as daytime,
        0 as payrollorder,
        'advance' as typeclass,
       b.advance_id
    from userstart a
    join ANALYTIC_DB.DBT_marts.disbursements b
    on a.user_id = b.user_id
    where b.product = 'Extra Cash' and b.disbursement_ds_pst > date('2022-12-15')
),
combined as
(
    select *
    from (
    select * from payroll_info
    union all
    select * from requests
    )
    order by user_id, daytime
),
advance_pay as
(
    select
      user_id,
      daytime,
      payrollorder,
      typeclass,
      sum(payrollorder) over (partition by user_id order by daytime asc rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as totalorder
    from combined
),
advance as
(
    select * from advance_pay where typeclass = 'advance'
),
pay as
(
    select * from advance_pay where typeclass = 'pay'
)
select * from pay
----
---solution

select after_pay_diff, count(distinct advance_id) as advancetaken from DBT.DEV_YANCHUN_PUBLIC.multi_draw_payroll
group by after_pay_diff
order by after_pay_diff
