DROP table IF EXISTS DBT.DEV_YANCHUN_PUBLIC.multi_draw_chime_dedup;
CREATE TABLE  DBT.DEV_YANCHUN_PUBLIC.multi_draw_chime_dedup AS
WITH USERSET AS
(
    SELECT
        USER_ID,
        DATE(PV_TS) AS STARTDATE
    FROM ANALYTIC_DB.DBT_marts.new_user_attribution
    WHERE STARTDATE > DATE('2022-12-15')
),
chime_raw AS
(SELECT
        USERSET.USER_ID AS USER_ID,
       bal.timestamp::DATE AS balance_date,
       bal.bank_account_id AS bank_account_id,
       bal.bank_connection_id AS bank_connection_id,
       COALESCE(bal."CURRENT", bal.available) AS balance,
       bal.timestamp AS bal_timestamp
    FROM SECONDARY_APP_DB.PUBLIC.BALANCE_LOG bal
    JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT bank_acct
    ON bal.bank_account_id = bank_acct.id
    JOIN USERSET
    ON bal.user_id = USERSET.USER_ID
    WHERE bank_acct.institution_id in (268940, 104812, 271751)
    AND bal.timestamp::DATE > DATE('2022-12-15')
    and balance < -0.001
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bal.user_id, bank_account_id, balance_date ORDER BY bal_timestamp DESC) = 1
    ORDER BY USER_ID
 )
SELECT
USER_ID, bank_account_id, balance_time, balance_value
FROM chime_raw, table(DBT.DEV_YANCHUN_PUBLIC.chime_remove_duplicate(balance_date, balance) over (partition by USER_ID, bank_account_id order by balance_date))
order by USER_ID, bank_account_id, balance_time;

