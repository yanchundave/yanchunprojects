WITH ref_date AS (
    SELECT * FROM {{ ref('dim_chime_ref_date') }}
), balance_log AS (
	SELECT * FROM {{ source('bank_data_service', 'BALANCE_LOG') }}
), bank_acct AS (
	SELECT * FROM {{ source('dave', 'BANK_ACCOUNT') }}
), ref_date_range AS (
    SELECT MAX(ref_date) AS max_ref_date,
           MIN(ref_date) AS min_ref_date
      FROM ref_date
)
SELECT bal.user_id AS user_id,
       bal.timestamp::DATE AS balance_date,
       bal.bank_account_id AS bank_account_id,
       bal.bank_connection_id AS bank_connection_id,
       bal.avail AS available_balance,
       bal.curr AS current_balance,
       bal.caller AS caller,
       bal.timestamp AS bal_timestamp,
       bal.rowkey AS uuid
  FROM balance_log bal
       JOIN bank_acct
       ON bal.bank_account_id = bank_acct.id
       CROSS JOIN ref_date_range
 WHERE bank_acct.institution_id in (268940, 104812, 271751)
   AND bal.timestamp::DATE BETWEEN ref_date_range.min_ref_date - 180 AND ref_date_range.max_ref_date
QUALIFY ROW_NUMBER() OVER (PARTITION BY bal.user_id, bank_account_id, balance_date ORDER BY bal_timestamp DESC) = 1
