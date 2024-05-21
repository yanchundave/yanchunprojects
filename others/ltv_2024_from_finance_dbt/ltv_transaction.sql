{{ config(
    materialized = 'table',
    tags = ["ltv"]
) }}

WITH user_pv AS
(
    SELECT
    USER_ID,
    TO_DATE(PV_TS) AS STARTDATE
    FROM {{ source('dbt_marts', 'NEW_USER_REATTRIBUTION') }}
    WHERE to_date(PV_TS) >= GREATEST(DATE('2021-01-01'), DATEADD('month', -24, DATE_TRUNC('month', CURRENT_DATE())))
),
advance_records AS
(
    SELECT
    TO_VARCHAR(advance_id) AS TRANS_ID,
    user_pv.USER_ID AS USER_ID,
    user_pv.STARTDATE AS STARTDATE,
    TO_DATE(advance_disbursement_time_utc) AS TRANS_TIME,
    adv_fee_latest + adv_tip_latest AS REVENUE
    FROM user_pv
    LEFT JOIN {{ ref('dim_advance_flat') }} advance
    ON user_pv.USER_ID = advance.USER_ID
),
overdraft_records AS
(
    SELECT
    overdraft_id AS TRANS_ID,
    user_pv.USER_ID AS USER_ID,
    user_pv.startdate AS STARTDATE,
    TO_DATE(overdraft_disbursement_time_utc) AS TRANS_TIME,
    overdraft_service_fee_latest +
    overdraft_express_fee_latest +
    overdraft_user_tip_latest AS REVENUE
    FROM user_pv
    LEFT JOIN {{ ref('dim_overdraft_flat') }} overdraft
    ON user_pv.USER_ID = overdraft.USER_ID
)

SELECT * FROM advance_records
UNION ALL
SELECT * FROM overdraft_records
