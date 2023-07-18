CREATE OR REPLACE TABLE DBT.DEV_YANCHUN_PUBLIC.competitors_not_taken AS

WITH dates_acc AS (
	SELECT
        DATE('2023-01-01') AS start_date,
        CURRENT_DATE() - 1 AS end_date
),
plaid_txn_raw AS (
	SELECT * FROM datastream_prd.dave.bank_transaction
),
plaid_data AS
(
    SELECT * FROM datastream_prd.dave.bank_transaction
),
requests AS
(
    SELECT * FROM ANALYTIC_DB.DBT_MARTS.REQUESTS
),
disbursement AS
(
    SELECT * FROM ANALYTIC_DB.DBT_MARTS.disbursements
),
bank_connection AS
(
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION
),
bank_connection_newuser AS
(
    SELECT
        user_id,
        MIN(DATE(initial_pull)) AS date_of_ref
    FROM bank_connection
    CROSS JOIN dates_acc
    WHERE initial_pull BETWEEN dates_acc.start_date AND dates_acc.end_date
    GROUP BY user_id
),

newuser AS
(
    SELECT
        bank_connection_newuser.user_id,
        bank_connection_newuser.date_of_ref,
        SUM(COALESCE(requests.is_approved, 0)) AS is_approved_sum,
        SUM(COALESCE(requests.is_disbursed, 0)) AS is_disbursed_sum
    FROM bank_connection_newuser
    JOIN requests
    ON bank_connection_newuser.user_id = requests.user_id
        and requests.requested_ds_pst BETWEEN  bank_connection_newuser.date_of_ref AND bank_connection_newuser.date_of_ref +  interval '30 days'
    GROUP BY 1, 2

),

newuserupdate AS
(
  SELECT
    a.user_id,
    a.date_of_ref,
    CASE
      WHEN b.user_id IS NULL THEN 'inactive'
      WHEN b.is_approved_sum = 0 and b.is_disbursed_sum = 0 THEN 'not_approval'
      WHEN b.is_approved_sum > 0 and b.is_disbursed_sum = 0 THEN 'not_taken'
      WHEN b.is_approved_sum > 0 and b.is_disbursed_sum > 0 THEN 'taken'
      ELSE 'other'
    END AS newmember_type
  FROM bank_connection_newuser a
  LEFT JOIN  newuser b
  ON a.user_id = b.user_id
  WHERE newmember_type = 'not_taken'
),

dave_approval AS
(
    SELECT
        a.user_id,
        a.date_of_ref,
        a.newmember_type,
        'dave_request' as competitor_name,
        requested_ds_pst as transaction_date,
        max_approved_amount as amount
    FROM newuserupdate a
    JOIN requests
    ON a.user_id = requests.user_id
        and requests.requested_ds_pst BETWEEN  a.date_of_ref AND a.date_of_ref +  interval '30 days'


),

plaid_user_bc AS
(
    SELECT
        a.user_id,
        a.date_of_ref,
        a.newmember_type,
        CASE WHEN LOWER(b.display_name) LIKE 'albert instant%' OR LOWER(b.display_name) LIKE 'albert savings%' THEN 'Albert'
           WHEN LOWER(b.display_name) LIKE '%brigit%' THEN 'Brigit'
           WHEN LOWER(b.display_name) LIKE '%empower%' THEN 'Empower'
           WHEN LOWER(b.display_name) LIKE '%earnin%' AND LOWER(b.display_name) NOT LIKE '%learnin%' THEN 'EarnIn'
           WHEN LOWER(b.display_name) LIKE '%money%lion%' THEN 'Money Lion'
           WHEN lower(display_name) like '%cash app%cash out%' then 'Cash App'
           ELSE NULL
        END as competitor_name,
        b.transaction_date,
        b.amount
    FROM newuserupdate a
    JOIN plaid_txn_raw b
    on a.user_id = b.user_id
    and b.transaction_date >= a.date_of_ref and b.transaction_date <= a.date_of_ref + interval '30 days'
    where competitor_name is not NULL and b.amount > 0
),

final as (
    select
        user_id,
        date_of_ref,
        newmember_type,
        array_to_string(array_agg(competitor_name) within group (order by transaction_date), ',') as competitor_list,
        array_to_string(array_agg(amount) within group (order by transaction_date), ',') as amount_list
    from
    (
        select * from dave_approval
        union all
        select * from plaid_user_bc
    ) a
    group by 1, 2, 3
)
select * from final