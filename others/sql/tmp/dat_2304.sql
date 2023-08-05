


--------------------

1. average monthly Dave spend

WITH TRANS AS (
  SELECT * ,
    DATE_TRUNC('MONTH', TRANSACTION_DS) AS MONTH_NUM
  FROM ANALYTIC_DB.DBT_MARTS.FCT_TRANSACTIONS
  where TRANSACTION_DS >= DATE_TRUNC('MONTH', DATEADD('MONTH', -6, CURRENT_DATE()))
  AND TRANSACTION_AMOUNT < 0
),
USERS AS (
  SELECT MID as USER_ID FROM SANDBOX.DEV_YYANG.USERLIST2304
),
DAVESPENDING AS (
  SELECT
    TRANS.*
  FROM USERS
  JOIN TRANS
  ON USERS.USER_ID = TRANS.USER_ID
)
SELECT MONTH_NUM, SUM(-TRANSACTION_AMOUNT) / COUNT(distinct USER_ID)
FROM DAVESPENDING
GROUP BY MONTH_NUM
ORDER BY MONTH_NUM

-----dave spending debit

WITH TRANS AS (
  SELECT * ,
    DATE_TRUNC('MONTH', TRANSACTION_DS) AS MONTH_NUM
  FROM ANALYTIC_DB.DBT_MARTS.FCT_TRANSACTIONS
  where TRANSACTION_DS >= DATE_TRUNC('MONTH', DATEADD('MONTH', -6, CURRENT_DATE()))
  AND TRANSACTION_AMOUNT < 0
),
USERS AS (
  SELECT MID as USER_ID FROM SANDBOX.DEV_YYANG.USERLIST2304
),
DAVESPENDING AS (
  SELECT
    TRANS.*
  FROM USERS
  JOIN TRANS
  ON USERS.USER_ID = TRANS.USER_ID
)
SELECT MONTH_NUM, SUM(-TRANSACTION_AMOUNT) / COUNT(distinct USER_ID)
FROM DAVESPENDING
WHERE IS_SPEND_TXN = 1
GROUP BY MONTH_NUM
ORDER BY MONTH_NUM

------- total dexternal transaction

with plaid_data as
(
  select c.*,
   DATE_TRUNC('MONTH', TRANSACTION_DATE) AS MONTH_NUM
  from datastream_prd.dave.bank_transaction c
  join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT a
  on c.bank_account_id=a.id
  join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION b
  on a.bank_connection_id=b.id
  and b.BANKING_DATA_SOURCE='PLAID'
  where c.TRANSACTION_DATE > DATE_TRUNC('MONTH', DATEADD('MONTH', -6, CURRENT_DATE()))
  AND c.AMOUNT < 0
),
USERS AS (
  SELECT MID as USER_ID FROM SANDBOX.DEV_YYANG.USERLIST2304
),
TOTALSPENDING AS
(
  SELECT
    plaid_data.*
  FROM plaid_data
  join USERS
  ON plaid_data.user_id = USERS.user_id
  WHERE PENDING = FALSE
)
SELECT MONTH_NUM, SUM(-AMOUNT) / COUNT(distinct USER_ID)
FROM TOTALSPENDING
GROUP BY MONTH_NUM
ORDER BY MONTH_NUM

-----This is a query provided by Michael, drafted by Hu about the debit spending total

with transaction_category as (
    SELECT
        date_trunc('month',transaction_date) as transaction_month,
        case when plaid_category ilike '%shops%clothing%' then 'debit'--'Clothing Stores'
          when plaid_category ilike '%restaurants%' then 'debit'--'Restaurants'
          when plaid_category ilike '%car%rental%' then 'debit'--'Car and Truck Rentals'
          when plaid_category ilike '%groceries%'
            OR plaid_category ilike '%food%and%beverage%'
            OR plaid_category ilike '%convenience%stores%' then 'debit'--'Groceries'
          when plaid_category ilike '%car%dealer%' then 'Car Dealers and Leasing'
          when plaid_category ilike '%digital%purchase%' then 'debit'--'Digital Purchase'
          when plaid_category ilike '%gas%station%' then 'debit'--'Gas Stations'
          when plaid_category ilike '%travel%' then 'debit'--'Travel (excluding gas)'
          when plaid_category ilike '%payment%rent%' then 'non-debit'--'Rent'
          --when plaid_category ilike '%loans%and%mortgages%' then 'Loans and Mortgages'
          when plaid_category ilike '%credit card%' then 'non-debit'--'Credit Card Payment'
          when plaid_category ilike '%insurance%' then 'non-debit'--'insurance'
          when plaid_category ilike '%venmo%'
            OR plaid_category ilike '%paypal%' then 'non-debit'--'P2P (Venmo or Paypal)'
          else 'other' end as spending_categories,
        amount,
        c.user_id

FROM datastream_prd.dave.bank_transaction C
join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT a
on c.bank_account_id=a.id
  join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION b
  on a.bank_connection_id=b.id
  and b.BANKING_DATA_SOURCE='PLAID' --removing bank of dave transactions and only looking at plaid
WHERE transaction_date >= '2022-01-01'
    AND amount between -9999 and 0 --arbitrarily filtering out single transactions greater than $10k
    and pending_display_name!='PENDING DEBIT/HOLD'
)
Select transaction_month,
	total_spend_amount,
    count(distinct tt.user_id) as total_users
from (
select
    tc.user_id,
    transaction_month,
    spending_categories,
    case when abs(SUM(amount)) >= 0 and abs(SUM(amount)) <= 99    then '  0 - 99'
           when abs(SUM(amount)) >= 100 and abs(SUM(amount)) <= 199    then '  100 - 199'
           when abs(SUM(amount)) >= 200 and abs(SUM(amount)) <= 299    then '  200 - 299'
           when abs(SUM(amount)) >= 300 and abs(SUM(amount)) <= 399    then '  300 - 399'
           when abs(SUM(amount)) >= 400 and abs(SUM(amount)) <= 499    then '  400 - 499'
           when abs(SUM(amount)) >= 500 and abs(SUM(amount)) <= 599    then '  500 - 599'
           when abs(SUM(amount)) >= 600 and abs(SUM(amount)) <= 799    then '  600 - 799'
           when abs(SUM(amount)) >= 800 and abs(SUM(amount)) <= 999    then '  800 - 999'
           when abs(SUM(amount)) >= 1000 and abs(SUM(amount)) <= 1499    then '  1000 - 1499'
           else '1500+'
      end total_spend_amount
    --count(*) as total_transactions_cnt
from transaction_category tc
LEFT JOIN
(
select ot.user_id
from ANALYTIC_DB.DBT_marts.dim_advance_users__overdrafts_taken ot
where disbursement_ds_pst >= current_date()-30
		AND overdraft_amount > 100
) ou on tc.user_id = ou.user_id
where spending_categories = 'debit'
	AND ou.user_id is not null
group by 1,2,3
) tt
group by 1,2

-----Plaid Debit from Yanchun

with transaction_category as (
    SELECT
        date_trunc('month',transaction_date) as transaction_month,
        case when plaid_category ilike '%shops%clothing%' then 'debit'--'Clothing Stores'
          when plaid_category ilike '%restaurants%' then 'debit'--'Restaurants'
          when plaid_category ilike '%car%rental%' then 'debit'--'Car and Truck Rentals'
          when plaid_category ilike '%groceries%'
            OR plaid_category ilike '%food%and%beverage%'
            OR plaid_category ilike '%convenience%stores%' then 'debit'--'Groceries'
          when plaid_category ilike '%car%dealer%' then 'Car Dealers and Leasing'
          when plaid_category ilike '%digital%purchase%' then 'debit'--'Digital Purchase'
          when plaid_category ilike '%gas%station%' then 'debit'--'Gas Stations'
          when plaid_category ilike '%travel%' then 'debit'--'Travel (excluding gas)'
          when plaid_category ilike '%payment%rent%' then 'non-debit'--'Rent'
          --when plaid_category ilike '%loans%and%mortgages%' then 'Loans and Mortgages'
          when plaid_category ilike '%credit card%' then 'non-debit'--'Credit Card Payment'
          when plaid_category ilike '%insurance%' then 'non-debit'--'insurance'
          when plaid_category ilike '%venmo%'
            OR plaid_category ilike '%paypal%' then 'non-debit'--'P2P (Venmo or Paypal)'
          else 'other' end as spending_categories,
        amount,
        c.user_id

FROM datastream_prd.dave.bank_transaction C
join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT a
on c.bank_account_id=a.id
  join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_CONNECTION b
  on a.bank_connection_id=b.id
  and b.BANKING_DATA_SOURCE='PLAID' --removing bank of dave transactions and only looking at plaid
WHERE transaction_date  >= DATE_TRUNC('MONTH', DATEADD('MONTH', -6, CURRENT_DATE()))
    AND amount between -9999 and 0 --arbitrarily filtering out single transactions greater than $10k
    and pending_display_name!='PENDING DEBIT/HOLD'
),
USERS AS (
  SELECT MID as USER_ID FROM SANDBOX.DEV_YYANG.USERLIST2304
),
debitspending as
(
  select
  a.*

  from transaction_category a
  join USERS b
  on a.user_id = b.user_id
)
select
transaction_month,
spending_categories,
sum(-amount)/count(distinct user_id)
from debitspending
group by 1, 2

-------
--disbursement
---
with disbursement as (
  SELECT *,
    date_trunc('month', disbursement_ds_pst) as transaction_month
   FROM ANALYTIC_DB.DBT_MARTS.disbursements
   where disbursement_ds_pst > DATE_TRUNC('MONTH', DATEADD('MONTH', -6, CURRENT_DATE()))
),
USERS AS (
  SELECT MID as USER_ID FROM SANDBOX.DEV_YYANG.USERLIST2304
),
disburse_user as (
  select
    disbursement.*
  from disbursement
  join USERS
  on disbursement.user_id = USERS.USER_ID
)
select transaction_month, sum(taken_amount)/count(distinct user_id)
from disburse_user
group by 1
order by transaction_month