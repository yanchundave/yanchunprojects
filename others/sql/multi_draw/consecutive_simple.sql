DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_consecutive;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_consecutive as
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
        LAG(FUNDING_DATE, -1) OVER (PARTITION BY USERSET.USER_ID ORDER BY FUNDING_DATE) as next_funding_date,
        LAG(t_amount, -1) OVER (PARTITION BY USERSET.USER_ID ORDER BY FUNDING_DATE) as next_funding_amount
    from ANALYTIC_DB.DBT_MARTS.disbursements disburse
    join USERSET
    ON disburse.USER_ID = USERSET.USER_ID
    WHERE DISBURSEMENT_DS_PST > DATE('2022-12-15') AND PRODUCT='Extra Cash'
),
advance_update as
(
    select * from advance where DATEDIFF(DAY, FUNDING_DATE, next_funding_date) <= 28

),
chime as
(
    select
        a.user_id,
        a.funding_date,
        a.next_funding_date,
        a.advance_id,
        a.next_funding_amount,
        a.r_amount,
        a.t_amount,
        max(-1 * b.balance) as chime_amount,
        max(1) as chime_count
    from advance_update a
    left join chime_raw b
    on a.user_id = b.user_id
    where b.balance_date between a.funding_date and a.next_funding_date
    group by 1,2,3,4,5,6,7
),
nonchime as
(
    select
        a.user_id,
        a.funding_date,
        a.next_funding_date,
        a.advance_id,
        a.next_funding_amount,
        a.r_amount,
        a.t_amount,
        sum(advance_volume) as nonchime_amount,
        count(transaction_date) as nonchime_count
    from advance_update a
    left join nonchime_raw b
    on a.user_id = b.user_id
    where b.transaction_date between a.funding_date and a.next_funding_date
    group by 1,2,3,4,5,6,7
)
select
    COALESCE(a.user_id, b.user_id) as user_id,
    COALESCE(a.funding_date, b.funding_date) as funding_date,
    COALESCE(a.next_funding_date, b.next_funding_date) as next_funding_date,
    COALESCE(a.advance_id, b.advance_id) as advance_id,
    COALESCE(a.next_funding_amount, b.next_funding_amount) as next_funding_amount,
    COALESCE(a.r_amount, b.r_amount) as r_amount,
    COALESCE(a.t_amount, b.t_amount) as t_amount,
    case when a.chime_count is null then 0 else a.chime_count end as chime_count,
    case when a.chime_amount is null then 0 else a.chime_amount end as chime_amount,
    case when b.nonchime_count is null then 0 else b.nonchime_count end as nonchime_count,
    case when b.nonchime_amount is null then 0 else b.nonchime_amount end as nonchime_amount
from chime a full join nonchime b
on a.user_id  = b.user_id and a.advance_id = b.advance_id


------solution sql

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
