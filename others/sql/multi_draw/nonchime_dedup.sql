DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_nonchime_dedup;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_nonchime_dedup AS
WITH USERSET AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
NONCHIME AS
(
    SELECT
        USERSET.USER_ID,
        transaction_date,
        DBT.DEV_YANCHUN_PUBLIC.UDF_NONCHIME_COMPETITOR(display_name) AS nonchime_competitor_name,
        amount
    FROM DATASTREAM_PRD.DAVE.BANK_TRANSACTION plaid_txn
    JOIN USERSET
    ON plaid_txn.USER_ID = USERSET.USER_ID
    WHERE plaid_txn.user_id IS NOT NULL
    AND nonchime_competitor_name IS NOT NULL
    and amount > 0
    and transaction_date > DATE('2022-12-15')
),
NONCHIME_DEDUP AS
(
    SELECT DISTINCT * FROM NONCHIME
),
NONCHIME_UPDATE AS
(
    SELECT
        USER_ID,
        transaction_date,
        nonchime_competitor_name,
        sum(amount) AS advance_volume
    FROM NONCHIME_DEDUP
    GROUP BY USER_ID, transaction_date, nonchime_competitor_name
)
SELECT * FROM NONCHIME_UPDATE;