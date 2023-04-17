WITH USERSET AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
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
    a.user_id,
    a.funding_date,
    a.advance_id,
    a.r_amount,
    a.t_amount,
    b.HAS_CHIME_SPOTME_N60D as chime_cnt,
    -1* b.LOWEST_CHIME_BAL_N60D_AMT_USD as chime_amt,
    c.nonchime_competitor_funding_txn_n60d_cnt as nonchime_cnt,
    c.NONCHIME_COMPETITOR_FUNDING_TXN_N60D_AMT_USD as nonchime_amt
  from advance  a
  left join DBT.ADV_CHURN_MARTS.FCT_CHIME_NEG_BAL b
  on a.user_id = b.user_id
  and a.funding_date = b.ref_date
  left join DBT.ADV_CHURN_MARTS.FCT_NONCHIME_COMPETITOR_TXN c
  on a.user_id = c.user_id and a.funding_date = c.ref_date
),
chimeupdate as
(
  select
    user_id,
    advance_id,
    funding_date,
    r_amount,
    t_amount,
    chime_cnt + nonchime_cnt as competitor_cnt,
    chime_amt + nonchime_amt as competitor_amt
  from chime
)
select count(distinct user_id), sum(r_amount - t_amount)/count(distinct user_id),
sum(t_amount)/count(distinct user_id), sum(competitor_cnt)/count(distinct user_id),
sum(competitor_amt)/count(distinct user_id)
from chimeupdate where r_amount > t_amount and competitor_amt > 0


------
WITH USERSET AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
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
)
select count(distinct user_id), sum(r_amount - t_amount), sum(r_amount - t_amount)/count(distinct user_id) as avg_diff from advance where r_amount > t_amount

---solution

----- soliton sql
with
raw as
(
select *,
  chime_count + nonchime_count as competitor_count,
  chime_amount + nonchime_amount as competitor_amount
  from DBT.DEV_YANCHUN_PUBLIC.multi_draw_consecutive
)
select
count(distinct user_id) as user_dave, sum(t_amount) / count(distinct user_id) as avg_dave, sum(competitor_count) as competitorcount, avg(competitor_count) as avg_compeitor_count,

sum(competitor_amount)/count(distinct user_id) as avg_competitor, sum(competitor_amount)/(sum(competitor_count)) as avg_competitor_per_count

from raw
where r_amount > t_amount
