--Advance transaction information
select 
advance_id, 
user_id, 
advance_disbursement_time_utc,
adv_fee_orig + adv_tip_orig as revenue
from DEV_SOLIN_FINANCE.dim_advance_flat;


--Extra cash transaction information
select 
overdraft_id, 
user_id, 
overdraft_disbursement_time_utc,
overdraft_service_fee_orig +
overdraft_express_fee_orig +
overdraft_user_tip_orig as revenue
from DEV_SOLIN_FINANCE.dim_overdraft_flat；

--How to handle timestamp
select date(advance_disbursement_time_utc), datediff('day',date(advance_disbursement_time_utc), date('2022-05-31')) 
from DEV_SOLIN_FINANCE.dim_advance_flat limit 10;


-- Recency, Frenquency, Monetary, T from advance user
with advance_records as 
(
select 
advance_id,
user_id, 
advance_disbursement_time_utc,
adv_fee_orig + adv_tip_orig as revenue
from DEV_SOLIN_FINANCE.dim_advance_flat
)
select 
user_id, 
count(advance_id) - 1 as frequency, 
datediff('day', date(min(advance_disbursement_time_utc)), date('2022-05-31')) as T, 
datediff('day', date(min(advance_disbursement_time_utc)), date(max(advance_disbursement_time_utc))) as recency,
avg(revenue) as monetary
from advance_records
where date(advance_disbursement_time_utc) <= date('2022-05-31')
group by user_id; 

-- Recency, Frenquency, Monetary, T from extra cash user
with overdraft_records as 
(
select 
overdraft_id, 
user_id, 
overdraft_disbursement_time_utc,
overdraft_service_fee_orig +
overdraft_express_fee_orig +
overdraft_user_tip_orig as revenue
from DEV_SOLIN_FINANCE.dim_overdraft_flat
)
select 
user_id, 
count(overdraft_id) - 1 as frequency, 
datediff('day', date(min(overdraft_disbursement_time_utc)), date('2022-05-31')) as T, 
datediff('day', date(min(overdraft_disbursement_time_utc)), date(max(overdraft_disbursement_time_utc))) as recency,
avg(revenue) as monetary
from overdraft_records
where date(overdraft_disbursement_time_utc) <= date('2022-05-31')
group by user_id; 

--- Get all the transaction
DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022;
CREATE table DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
AS
with user_pv AS 
(
    SELECT 
    USER_ID, 
    TO_DATE(PV_TS) AS startdate
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
    WHERE to_date(PV_TS) >= DATE('2021-01-01') 
    AND to_date(PV_TS) < DATE('2022-01-01')
),
advance_records AS 
(
    SELECT 
    TO_VARCHAR(advance_id) AS trans_id, 
    user_pv.USER_ID AS user_id, 
    user_pv.startdate AS startdate,
    TO_DATE(advance_disbursement_time_utc) AS trans_time,
    adv_fee_orig + adv_tip_orig AS revenue
    FROM DBT.DEV_SOLIN_FINANCE.dim_advance_flat advance
    JOIN user_pv 
    ON user_pv.USER_ID = advance.user_id
), 
overdraft_records AS 
(
    SELECT 
    overdraft_id AS trans_id, 
    user_pv.USER_ID AS user_id, 
    user_pv.startdate AS startdate,
    TO_DATE(overdraft_disbursement_time_utc) AS trans_time,
    overdraft_service_fee_orig + 
    overdraft_express_fee_orig +
    overdraft_user_tip_orig AS revenue
    FROM DBT.DEV_SOLIN_FINANCE.dim_overdraft_flat overdraft
    JOIN user_pv
    ON user_pv.USER_ID = overdraft.user_id
)
SELECT * FROM advance_records
UNION ALL
SELECT * FROM overdraft_records;
    