--Update multi-draw related

--- UDF

CREATE OR REPLACE FUNCTION SANDBOX.DEV_YYANG.UDF_NONCHIME_COMPETITOR(description STRING) RETURNS STRING AS
$$
    -- Top competitior: Albert, Brigit, Empower, Earnin --
    -- exclued chime since it is unique --
    CASE WHEN LOWER(description) LIKE 'albert instant%' OR LOWER(description) LIKE 'albert savings%' THEN 'Albert'
         WHEN LOWER(description) LIKE '%brigit%' THEN 'Brigit'
         WHEN LOWER(description) LIKE '%empower%' THEN 'Empower'
         WHEN LOWER(description) LIKE '%earnin%' AND LOWER(description) NOT LIKE '%learnin%' THEN 'EarnIn'
         WHEN LOWER(description) LIKE '%money%lion%' THEN 'Money Lion'
         --WHEN LOWER(description) LIKE '%varo%' THEN 'Varo'
         WHEN lower(description) like '%cash app%cash out%' then 'Cash App'
         ELSE NULL
    END
$$;

---- HANDLE CHIME BALANCE VALUE

CREATE OR REPLACE FUNCTION SANDBOX.DEV_YYANG.chime_remove_duplicate( balance_date DATE, balance FLOAT)
returns table(balance_time DATE, balance_value FLOAT)
language python
runtime_version=3.8
handler='ChimeRemoveDuplicate'
as $$
class ChimeRemoveDuplicate:
    def __init__(self):
        self._currentamount = 0


    def process(self, balance_date, balance):
        if self._currentamount == 0:
            self._currentamount = balance
            yield(balance_date, -1 * balance)

        elif balance != self._currentamount:
            if balance < self._currentamount:
                diff = self._currentamount - balance
                self._currentamount = balance
                yield(balance_date, diff)
            else:
                 self._currentamount = balance
        else:
            self._currentamount = balance
            yield None

    def end_partition(self):
        yield None
$$;

-----RAW TABLE
CREATE OR REPLACE TABLE SANDBOX.DEV_YYANG.MULTI_DRAW_RAW AS
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
        SANDBOX.DEV_YYANG.UDF_NONCHIME_COMPETITOR(display_name) AS nonchime_competitor_name,
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
),
chime_raw AS
(SELECT
        USERSET.USER_ID AS USER_ID,
       bal.timestamp::DATE AS balance_date,
       bal.bank_account_id AS bank_account_id,
       bal.bank_connection_id AS bank_connection_id,
       COALESCE(bal."CURR", bal.avail) AS balance,
       bal.timestamp AS bal_timestamp
    FROM dave.bank_data_service.balance_log bal
    JOIN APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT bank_acct
    ON bal.bank_account_id = bank_acct.id
    JOIN USERSET
    ON bal.user_id = USERSET.USER_ID
    WHERE bank_acct.institution_id in (268940, 104812, 271751)
    AND bal.timestamp::DATE > DATE('2022-12-15')
    and balance < -0.001
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bal.user_id, bank_account_id, balance_date ORDER BY bal_timestamp DESC) = 1
    ORDER BY USER_ID
 ),
 chime_dedup_update as (
   SELECT
        USER_ID,
     bank_account_id,
     balance_time,
     balance_value
   FROM chime_raw, table(SANDBOX.DEV_YYANG.chime_remove_duplicate(balance_date, balance) over (partition by USER_ID, bank_account_id order by balance_date))
   order by USER_ID, bank_account_id, balance_time
 ),
 ADVANCE AS
(
    SELECT
        USERSET.USER_ID AS USER_ID,
        DISBURSEMENT_DS_PST AS FUNDING_DATE,
        'ADVANCE_DAVE' AS FUNDING_NAME,
        MAX_APPROVED_AMOUNT AS R_AMOUNT,
        TAKEN_AMOUNT AS T_AMOUNT
    from ANALYTIC_DB.DBT_MARTS.disbursements disburse
    join USERSET
    ON disburse.USER_ID = USERSET.USER_ID
    WHERE DISBURSEMENT_DS_PST > DATE('2022-12-15') AND PRODUCT='Extra Cash'
),
NONCHIME_NEW AS
(
    SELECT
        USER_ID,
        TRANSACTION_DATE AS FUNDING_DATE,
        NONCHIME_COMPETITOR_NAME AS FUNDING_NAME,
        0 AS R_AMOUNT,
        ADVANCE_VOLUME AS T_AMOUNT
    FROM NONCHIME_UPDATE
),
CHIME AS
(
    SELECT
        USER_ID,
        BALANCE_TIME AS FUNDING_DATE,
        'CHIME' AS FUNDING_NAME,
        0 AS R_AMOUNT,
        BALANCE_VALUE AS T_AMOUNT
    FROM  chime_dedup_update

),
TMP AS
(
    SELECT * FROM ADVANCE
    UNION ALL
    SELECT * FROM CHIME
    UNION ALL
    SELECT * FROM NONCHIME_NEW
),
TMP1 AS
(
SELECT
    USER_ID,
    FUNDING_DATE,
    FUNDING_NAME,
    R_AMOUNT,
    T_AMOUNT,
    CASE
        WHEN FUNDING_NAME = 'ADVANCE_DAVE' THEN 1
        ELSE 0
    END AS LABEL
FROM TMP
ORDER BY USER_ID, FUNDING_DATE, FUNDING_NAME
)
SELECT
    USER_ID,
    FUNDING_DATE,
    FUNDING_NAME,
    R_AMOUNT,
    T_AMOUNT,
    SUM(LABEL) over (PARTITION BY USER_ID order by FUNDING_DATE asc LABEL desc rows between unbounded preceding and current row) AS ordernumber
FROM TMP1

----CHECK TOTAL

SELECT FUNDING_NAME, SUM(T_AMOUNT) AS TOTAL
FROM SANDBOX.DEV_YYANG.MULTI_DRAW_RAW
GROUP BY 1
ORDER BY 1


----- THIS TABLE IS NEEDED BY SOME REASON

DROP table IF EXISTS SANDBOX.DEV_YYANG.multi_draw_consecutive;
CREATE TABLE  SANDBOX.DEV_YYANG.multi_draw_consecutive AS
WITH DAVE_ADVANCE AS
(
    SELECT *
    FROM SANDBOX.DEV_YYANG.multi_draw_raw
    WHERE FUNDING_NAME = 'ADVANCE_DAVE'
)
SELECT
    USER_ID,
    FUNDING_DATE,
    DATEDIFF('DAY',
    LAG(FUNDING_DATE, 1) OVER (PARTITION BY USER_ID ORDER BY FUNDING_DATE ASC),
    FUNDING_DATE) AS CONSECUTIVE_DIFF
FROM DAVE_ADVANCE

---- THIS TABLE IS NEEDED BY SOME REASON TOO

--CHIME DEDUP

DROP table IF EXISTS SANDBOX.DEV_YYANG.multi_draw_chime_dedup_update;
CREATE TABLE  SANDBOX.DEV_YYANG.multi_draw_chime_dedup_update AS
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
       COALESCE(bal."CURR", bal.avail) AS balance,
       bal.timestamp AS bal_timestamp
    FROM dave.bank_data_service.balance_log bal
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
SELECT * from chime_raw

---For consecutive support table

DROP table IF EXISTS SANDBOX.DEV_YYANG.multi_draw_nonchime_dedup;
CREATE TABLE  SANDBOX.DEV_YYANG.multi_draw_nonchime_dedup AS
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

---- FOR CONSECUTIVE

DROP table IF EXISTS SANDBOX.DEV_YYANG.multi_draw_consecutive;
CREATE TABLE  SANDBOX.DEV_YYANG.multi_draw_consecutive as
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
    select * from SANDBOX.DEV_YYANG.multi_draw_chime_dedup_update
),
nonchime_raw as
(
    select * from SANDBOX.DEV_YYANG.multi_draw_nonchime_dedup
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