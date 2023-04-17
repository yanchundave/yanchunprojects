SELECT distinct
    a.USER_ID
FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO a
  where event_type in ('home screen viewed', 'app to foreground', 'app to background',
  'extra cash tab selected', 'extra cash landing screen loaded', 'extra cash story screen loaded')
    and date(event_time) >= '2022-12-15'


--home screen - home screen viewed
--open app - 'app to foreground', 'app to background'
--ec tab - extra cash tab selected,extra cash landing screen loaded,extra cash story screen loaded

WITH userstart AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),

userset as
(
    select
    a.user_id,
    a.advance_id,
    a.disbursement_ds_pst::date as disbursement_date,
    least(a.settlement_due_ds_pst::date , b.last_payment_date::date) as settlement_due
    from ANALYTIC_DB.DBT_MARTS.disbursements a
    join ANALYTIC_DB.DBT_MARTS.settlements b
    on a.user_id = b.user_id and a.advance_id = b.advance_id
    join userstart c
    on c.user_id = a.user_id
    WHERE a.DISBURSEMENT_DS_PST > DATE('2022-12-15') AND a.PRODUCT='Extra Cash'
),
amplitude as
(
    select
     a.user_id,
     b.event_type
    from userset a
    join ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO b
    on a.user_id = b.user_id
    where date(event_time) >= '2022-12-15' and date(b.event_time) between a.disbursement_date and a.settlement_due and
    event_type in ('home screen viewed', 'app to foreground', 'app to background',
  'extra cash tab selected', 'extra cash landing screen loaded', 'extra cash story screen loaded')
)
select count(distinct user_id) from amplitude


--all users without considering to vist page
WITH userstart AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
userset as
(
    select
    a.user_id,
    a.advance_id,
    a.disbursement_ds_pst::date as disbursement_date,
    least(a.settlement_due_ds_pst::date , b.last_payment_date::date) as settlement_due
    from ANALYTIC_DB.DBT_MARTS.disbursements a
    join ANALYTIC_DB.DBT_MARTS.settlements b
    on a.user_id = b.user_id and a.advance_id = b.advance_id
    join userstart c
    on c.user_id = a.user_id
    WHERE a.DISBURSEMENT_DS_PST > DATE('2022-12-15') AND a.PRODUCT='Extra Cash'
)
select count(distinct user_id) from userset