with userbankconnection as
(
    select
        user_id,
        DATE(FIRST_BC_TS_PST) AS DATE_OF_REF
    FROM ANALYTIC_DB.DBT_marts.fct_new_user_journey
    WHERE FIRST_BC_TS_PST IS NOT NULL and DATE_OF_REF >= DATE('2022-01-01')
),
requests AS
(
    SELECT * FROM ANALYTIC_DB.DBT_MARTS.REQUESTS
),
disbursement AS
(
    SELECT * FROM ANALYTIC_DB.DBT_MARTS.disbursements
),
user_segment as
(
    SELECT
        USER_ID,
        DATE_OF_INTEREST,
        USER_LABEL
    FROM DBT.ADV_CHURN_MARTS.FCT_ADV_SEGMENT
    WHERE DATE_OF_INTEREST = CURRENT_DATE() - 1
),
chime AS
(
    SELECT * FROM DBT.ADV_CHURN_MARTS.FCT_CHIME_NEG_BAL
),
plaid_txn_raw AS (
	SELECT * FROM datastream_prd.dave.bank_transaction
),
userupdate as
(
    SELECT
        a.user_id,
        a.date_of_ref,
        b.user_label
    FROM userbankconnection a
    left join user_segment b
    on a.user_id = b.user_id
),
dave_lastapproval as
(
   SELECT
    a.user_id,
    a.date_of_ref,
    a.user_label,
    b.max_approved_amount,
    row_number() over (partition by a.user_id order by b.requested_ds_pst desc) as rownumber
   FROM userupdate a
   join requests b
    on a.user_id = b.user_id
  where b.max_approved_amount > 0
  qualify rownumber = 1
),
dave_lastdisburse as
(
    SELECT
    a.user_id,
    a.date_of_ref,
    a.user_label,
    b.taken_amount,
    row_number() over (partition by a.user_id order by b.disbursement_ds_pst desc) as rownumber
     FROM userupdate a
     join disbursement b
      on a.user_id = b.user_id
    where b.taken_amount > 0
    qualify rownumber = 1
),
dave_approval as
(
    SELECT
        a.user_id,
        MAX(requests.max_approved_amount) AS max_value,
        SUM(requests.max_approved_amount) AS sum_value,
        SUM(requests.is_approved) AS count_value
    FROM userupdate a
    JOIN requests
    ON a.user_id = requests.user_id
        and requests.requested_ds_pst BETWEEN  current_date() - interval '31 days' AND current_date() - 1
    GROUP BY 1
),
dave_disburse as
(
    SELECT
        a.user_id,
        MAX(b.taken_amount) AS max_value,
        SUM(b.taken_amount) AS sum_value,
        SUM(b.taken_amount) AS count_value
    FROM userupdate a
    JOIN disbursement b
    ON a.user_id = b.user_id
        and b.disbursement_ds_pst BETWEEN  current_date() - interval '31 days' AND current_date() - 1
    GROUP BY 1

),
dave_approval_union as
(
    select
        a.user_id,
        a.date_of_ref,
        a.user_label,
        'dave_approval' as advance_source,
        a.max_approved_amount as last_amount,
        b.max_value,
        b.sum_value,
        b.count_value
    from dave_lastapproval a
    left join dave_approval b
    on a.user_id = b.user_id
),

dave_disburse_union as
(
    select
        a.user_id,
        a.date_of_ref,
        a.user_label,
        'dave_disburse' as advance_source,
        a.taken_amount as last_amount,
        b.max_value,
        b.sum_value,
        b.count_value
    from dave_lastdisburse a
    left join dave_disburse b
    on a.user_id = b.user_id
),

chime_log as
(
    SELECT
        a.USER_ID,
        a.date_of_ref,
        a.user_label,
        'chime' as advance_source,
        0 as last_amount,
        -1 * LOWEST_CHIME_BAL_L30D_AMT_USD AS max_value,
        -1 * LOWEST_CHIME_BAL_L30D_AMT_USD AS sum_value,
        1 as count_value
    FROM userupdate a
    Join chime b
    on a.user_id = b.user_id
    WHERE b.REF_DATE = CURRENT_DATE() - 1
),

plaid_user_bc AS
(
    SELECT
        a.user_id,
        a.date_of_ref,
        a.user_label,
        CASE WHEN LOWER(b.display_name) LIKE 'albert instant%' OR LOWER(b.display_name) LIKE 'albert savings%' THEN 'Albert'
           WHEN LOWER(b.display_name) LIKE '%brigit%' THEN 'Brigit'
           WHEN LOWER(b.display_name) LIKE '%empower%' THEN 'Empower'
           WHEN LOWER(b.display_name) LIKE '%earnin%' AND LOWER(b.display_name) NOT LIKE '%learnin%' THEN 'EarnIn'
           WHEN LOWER(b.display_name) LIKE '%money%lion%' THEN 'Money Lion'
           WHEN lower(display_name) like '%cash app%gosq.com%' then 'Cash App'
           ELSE NULL
        END as advance_source,
        b.amount
    FROM userupdate a
    JOIN plaid_txn_raw b
    on a.user_id = b.user_id
    and b.transaction_date BETWEEN  current_date() - interval '31 days' AND current_date() - 1
    where advance_source is not NULL and b.amount > 0
),

plaid_user_group AS
(
    SELECT
        user_id,
        date_of_ref,
        user_label,
        advance_source,
        sum(0) as last_amount,
        max(amount) as max_value,
        sum(amount) as sum_value,
        count(amount) as count_value
    from plaid_user_bc
    group by 1, 2, 3, 4
)
select * from dave_approval_union
union all
select * from dave_disburse_union
union all
select * from chime_log
union all
select * from plaid_user_group