-- create a table with number of spending/funding txn in connected (plaid) accounts --
-- associated with nonchime competitors --
{{ config(
    materialized = 'incremental',
    unique_key = 'ref_date',
    incremental_strategy = 'delete+insert',
    cluster_by = ['ref_date'],
    tags = ["nonchime"]
) }}

{% macro create_udf_nonchime_competitor() %}

CREATE OR REPLACE FUNCTION {{ schema }}.UDF_NONCHIME_COMPETITOR(description STRING) RETURNS STRING AS
$$
    -- Top competitior: Albert, Brigit, Empower, Earnin --
    -- exclued chime since it is unique --
    CASE WHEN LOWER(description) LIKE 'albert instant%' OR LOWER(description) LIKE 'albert savings%' THEN 'Albert'
         WHEN LOWER(description) LIKE '%brigit%' THEN 'Brigit'
         WHEN LOWER(description) LIKE '%empower%' THEN 'Empower'
         WHEN LOWER(description) LIKE '%earnin%' AND LOWER(description) NOT LIKE '%learnin%' THEN 'EarnIn'
         WHEN LOWER(description) LIKE '%money%lion%' THEN 'Money Lion'
         ELSE NULL
    END
$$;

{% endmacro %}

WITH ref_date AS (
	SELECT * FROM {{ ref('dim_nonchime_ref_date') }}
), plaid_txn_raw AS (
	SELECT * FROM {{ source('datastream', 'bank_transaction') }}
), ref_date_range AS (
	SELECT MAX(ref_date) AS max_ref_date,
	       MIN(ref_date) AS min_ref_date
	  FROM ref_date
), plaid_txn AS (
    -- filter the data to make it smaller --
	SELECT *,
         {{ schema }}.UDF_NONCHIME_COMPETITOR(display_name) AS nonchime_competitor_name
	  FROM plaid_txn_raw
	       CROSS JOIN ref_date_range
	 WHERE transaction_date BETWEEN min_ref_date - 60 AND max_ref_date
	   AND {{ schema }}.UDF_NONCHIME_COMPETITOR(display_name) IS NOT NULL
)
SELECT plaid_txn.user_id AS user_id,
       ref_date.ref_date AS ref_date,
       ARRAY_AGG(DISTINCT nonchime_competitor_name) WITHIN GROUP (ORDER BY nonchime_competitor_name) AS nonchime_competitor_array,
       -- l3d --
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_l3d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_l3d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_l3d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_l3d_amt_usd,
       -- l7d --
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 7)
       	       AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_l7d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 7)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_l7d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 7)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_l7d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 7)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_l7d_amt_usd,
       -- l14d --
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 14)
               AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_l14d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 14)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_l14d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 14)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_l14d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 14)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_l14d_amt_usd,
       -- l30d --
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_l30d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_l30d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_l30d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_l30d_amt_usd,
       -- l60d --
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_l60d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_l60d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_l60d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_LAST_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_l60d_amt_usd,
       -- n3d --
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_n3d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_n3d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_n3d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 3)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_n3d_amt_usd,
       -- n30d --
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_n30d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_n30d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_n30d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 30)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_n30d_amt_usd,
       -- n60d --
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount > 0, 1, 0)) AS nonchime_competitor_funding_txn_n60d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount > 0, plaid_txn.amount, 0)) AS nonchime_competitor_funding_txn_n60d_amt_usd,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount < 0, 1, 0)) AS nonchime_competitor_spending_txn_n60d_cnt,
       SUM(IFF({{ schema }}.UDF_IS_NEXT_X_DAY(plaid_txn.transaction_date, ref_date.ref_date, 60)
               AND plaid_txn.amount < 0, abs(plaid_txn.amount), 0)) AS nonchime_competitor_spending_txn_n60d_amt_usd
  FROM plaid_txn
       CROSS JOIN ref_date ref_date
 WHERE plaid_txn.user_id IS NOT NULL
GROUP BY 1,2