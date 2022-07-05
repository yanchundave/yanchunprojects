
----- CREATE TABLE
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

----- EXTRACT TRAINING DATASET
with USER_TRANS AS 
    (
        SELECT 
        USER_ID, 
        TRANS_TIME,
        TRANS_ID,
        REVENUE
        FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
        WHERE to_date(TRANS_TIME) <= '2022-03-31'
    )
    SELECT 
    USER_ID, 
    DATE(MIN(TRANS_TIME)) AS first_trans,
    COUNT(TRANS_ID) - 1 AS frequency, 
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), DATE('2022-04-01')) AS T, 
    DATEDIFF('day', DATE(MIN(TRANS_TIME)), date(MAX(TRANS_TIME))) AS recency,
    AVG(REVENUE) AS monetary
    FROM USER_TRANS
    GROUP BY USER_ID;

---- Extract test dataset

SELECT USER_ID, COUNT(TRANS_ID) AS trans_num, SUM(REVENUE) AS real_revenue
FROM DBT.DEV_YANCHUN_PUBLIC.USER_TRANSACTION_2022
WHERE to_date(TRANS_TIME) >= date('2022-04-01')
GROUP BY USER_ID