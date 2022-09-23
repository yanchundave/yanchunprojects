--This is the sql for base tables which could be used by all the models
-- USE AT MOST TWO YEARS DATA OR AFTER 2021-01-01

DROP TABLE IF EXISTS DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022;
CREATE table DBT.DEV_YANCHUN_PUBLIC.LTV_USER_2022
AS
SELECT
USER_ID,
TO_DATE(PV_TS) AS STARTDATE,
PLATFORM,
ATTRIBUTION,
NETWORK,
BOD_ACCOUNT_OPEN_USER,
BOD_DIRECT_DEPOSIT_USER
FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION
WHERE to_date(PV_TS) >= GREATEST(DATE('2021-01-01'), DATEADD('month', -24, DATE_TRUNC('month', CURRENT_DATE())));

--THIS IS THE SQL FOR BASE TABLES WHICH CONTAINS TRANSACTION INFORMATION
DROP TABLE IF EXISTS ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022;
CREATE table ACCOUNTING.DBT_LOCAL.USER_TRANSACTION_2022
AS
with user_pv AS
(
    SELECT
    USER_ID,
    TO_DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_MARTS.NEW_USER_REATTRIBUTION

),
advance_records AS
(
    SELECT
    TO_VARCHAR(advance_id) AS TRANS_ID,
    user_pv.USER_ID AS USER_ID,
    user_pv.STARTDATE AS STARTDATE,
    TO_DATE(advance_disbursement_time_utc) AS TRANS_TIME,
    adv_fee_orig + adv_tip_orig AS REVENUE
    FROM user_pv
    LEFT JOIN ACCOUNTING.DBT_PROD.DIM_ADVANCE_FLAT advance
    ON user_pv.USER_ID = advance.USER_ID
),
overdraft_records AS
(
    SELECT
    overdraft_id AS TRANS_ID,
    user_pv.USER_ID AS USER_ID,
    user_pv.startdate AS STARTDATE,
    TO_DATE(overdraft_disbursement_time_utc) AS TRANS_TIME,
    overdraft_service_fee_orig +
    overdraft_express_fee_orig +
    overdraft_user_tip_orig AS REVENUE
    FROM user_pv
    LEFT JOIN ACCOUNTING.DBT_PROD.DIM_OVERDRAFT_FLAT overdraft
    ON user_pv.USER_ID = overdraft.USER_ID
)
SELECT * FROM advance_records
UNION ALL
SELECT * FROM overdraft_records;

----USER BANK CONNECTION

DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION;
CREATE TABLE DBT.DEV_YANCHUN_PUBLIC.USER_BANK_CONNECTION AS
SELECT
DISTINCT bc.user_id,
CASE
  WHEN DISPLAY_NAME is null THEN 'blank'
  WHEN DISPLAY_NAME like 'Chime%' THEN 'CHIME'
  WHEN DISPLAY_NAME like 'Varo%' or DISPLAY_NAME ='Albert' or DISPLAY_NAME ='Step'   or DISPLAY_NAME like 'Go%Bank%'
  THEN 'other neo bank'
  ELSE 'traiditonal'
  END AS bank_category,
bc.id  AS bank_account_id,
RANK() OVER (PARTITION BY bc.USER_ID ORDER BY bc.UPDATED,bc.LAST_PULL, bc.INITIAL_PULL DESC) AS rank,
bc.HAS_VALID_CREDENTIALS,
HAS_TRANSACTIONS
FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT ba
LEFT JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE."BANK_CONNECTION" AS  bc
ON ba.user_id = bc.user_id AND ba.bank_connection_id = bc.id
           --  left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.INSTITUTION i on i.ID = ba.INSTITUTION_ID
LEFT JOIN analytic_db.dbt_marts.fct_advance_approvals ap
ON ap.user_id = bc.user_id AND ap.BANK_ACCOUNT_ID=bc.id
WHERE 1=1
AND bc.deleted IS NULL
AND ba.deleted IS NULL
AND ba._FIVETRAN_DELETED = 'false'
AND bc._FIVETRAN_DELETED = 'false'
AND bc.banking_data_source in ('PLAID', 'MX')
--     and bc.User_ID in ('1806882','1393948','11609725')
AND (ba.MICRO_DEPOSIT IN ('COMPLETED') or ba.MICRO_DEPOSIT is NULL)
qualify Rank = 1