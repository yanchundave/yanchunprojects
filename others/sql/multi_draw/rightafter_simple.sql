DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_rightafter_update;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_rightafter_update as
WITH USERSET AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
chime_raw as
(
    select * from DBT.DEV_YANCHUN_PUBLIC.multi_draw_chime_dedup_update
),
nonchime_raw as
(
    select * from DBT.DEV_YANCHUN_PUBLIC.multi_draw_nonchime_dedup
),
ADVANCE AS
(
    SELECT
        USERSET.USER_ID AS USER_ID,
        DISBURSEMENT_DS_PST AS FUNDING_DATE,
        advance_id,
        MAX_APPROVED_AMOUNT AS R_AMOUNT,
        TAKEN_AMOUNT AS T_AMOUNT,
        dateadd(day,9, disbursement_ds_pst) as next_funding_date
    from ANALYTIC_DB.DBT_MARTS.disbursements disburse
    join USERSET
    ON disburse.USER_ID = USERSET.USER_ID
    WHERE DISBURSEMENT_DS_PST > DATE('2022-12-15') AND PRODUCT='Extra Cash'
),
chime as
(
    select
        a.user_id,
        a.funding_date,
        a.next_funding_date,
        a.advance_id,
        a.r_amount,
        a.t_amount,
        max(-1 * b.balance) as chime_amount,
        max(1) as chime_count
    from advance a
    left join chime_raw b
    on a.user_id = b.user_id
    where b.balance_date between a.funding_date and a.next_funding_date
    group by 1,2,3,4,5,6
),
nonchime as
(
    select
        a.user_id,
        a.funding_date,
        a.next_funding_date,
        a.advance_id,
        a.r_amount,
        a.t_amount,
        sum(advance_volume) as nonchime_amount,
        count(transaction_date) as nonchime_count
    from advance a
    left join nonchime_raw b
    on a.user_id = b.user_id
    where b.transaction_date between a.funding_date and a.next_funding_date
    group by 1,2,3,4,5,6
)
select
    COALESCE(a.user_id, b.user_id) as user_id,
    COALESCE(a.funding_date, b.funding_date) as funding_date,
    COALESCE(a.next_funding_date, b.next_funding_date) as next_funding_date,
    COALESCE(a.advance_id, b.advance_id) as advance_id,
    COALESCE(a.r_amount, b.r_amount) as r_amount,
    COALESCE(a.t_amount, b.t_amount) as t_amount,
    case when a.chime_count is null then 0 else a.chime_count end as chime_count,
    case when a.chime_amount is null then 0 else a.chime_amount end as chime_amount,
    case when b.nonchime_count is null then 0 else b.nonchime_count end as nonchime_count,
    case when b.nonchime_amount is null then 0 else b.nonchime_amount end as nonchime_amount
from chime a full join nonchime b
on a.user_id  = b.user_id and a.advance_id = b.advance_id
---
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_simple;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_simple as
WITH USERSET AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
chime_raw as
(
    select * from dbt.adv_churn_marts.fct_chime_neg_bal
),
nonchime_raw as
(
    select * from dbt.adv_churn_marts.FCT_NONCHIME_COMPETITOR_TXN
),
ADVANCE AS
(
    SELECT
        USERSET.USER_ID AS USER_ID,
        DISBURSEMENT_DS_PST AS FUNDING_DATE,
        'ADVANCE_DAVE' AS FUNDING_NAME,
        advance_id,
        MAX_APPROVED_AMOUNT AS R_AMOUNT,
        TAKEN_AMOUNT AS T_AMOUNT
    from ANALYTIC_DB.DBT_MARTS.disbursements disburse
    join USERSET
    ON disburse.USER_ID = USERSET.USER_ID
    WHERE DISBURSEMENT_DS_PST > DATE('2022-12-15') AND PRODUCT='Extra Cash'
),
chime as
(
    select
        advance.user_id,
        advance.funding_date,
        advance.funding_name,
        advance.r_amount,
        advance.t_amount,
        advance.advance_id,
        chime_raw.has_chime_spotme_l7d,
        chime_raw.lowest_chime_bal_l7d_amt_usd
    from advance
    left join chime_raw
    on advance.user_id = chime_raw.user_id
    and dateadd(day, 7, advance.funding_date::date) = chime_raw.ref_date
),
nonchime as
(
    select
        chime.*,
        nonchime_raw.NONCHIME_COMPETITOR_FUNDING_TXN_L7D_CNT,
        nonchime_raw.NONCHIME_COMPETITOR_FUNDING_TXN_L7D_AMT_USD
    from chime
    left join nonchime_raw
    on chime.user_id = nonchime_raw.user_id
    and dateadd(day, 7, chime.funding_date::date) = nonchime_raw.ref_date
)
select
    user_id,
    funding_date,
    advance_id,
    r_amount,
    t_amount,
    has_chime_spotme_l7d + NONCHIME_COMPETITOR_FUNDING_TXN_L7D_CNT as competitor_count,
    lowest_chime_bal_l7d_amt_usd * -1 + NONCHIME_COMPETITOR_FUNDING_TXN_L7D_AMT_USD as competitor_amount
from nonchime

---solution sql
select count(distinct user_id) as userscount, sum(competitor_amount) as competitor_amount, sum(t_amount) as dave_amount, sum(competitor_amount)/count(distinct user_id) as competitor_avg, sum(t_amount)/count(distinct user_id) as dave_avg
  from DBT.DEV_YANCHUN_PUBLIC.multi_draw_simple
  where r_amount = t_amount and competitor_count > 0

--different percentile
select
  user_id,
  sum(r_amount) as request_amount,
  sum(t_amount) as taken_amount,
  sum(chime_amount) + sum(nonchime_amount) as competitor_amount
from DBT.DEV_YANCHUN_PUBLIC.multi_draw_rightafter_update
group by user_id