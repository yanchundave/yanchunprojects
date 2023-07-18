with exp_iw as (
select * from (
select user_id,
ASSIGNMENT,
COUNT(DISTINCT ASSIGNMENT) OVER (PARTITION by user_id) AS assignment_check,
min(valid_from) as min_valid_from
FROM ANALYTIC_DB.AMPLITUDE.EXPERIMENTS
where experiment='first-instant-withdrawal-fee-waived'
and assignment in ('control','enabled')
--AND VALID_FROM>='2023-02-15'
AND VALID_from<='2023-04-15'
GROUP BY 1,2
qualify assignment_check=1)
where min_valid_from>='2023-03-01'
--and assignment in ('control','enabled')

    ),

 exp_wayfinding as (
select * from (
select user_id,
ASSIGNMENT,
COUNT(DISTINCT ASSIGNMENT) OVER (PARTITION by user_id) AS assignment_check,
min(valid_from)  as min_valid_from
FROM ANALYTIC_DB.AMPLITUDE.EXPERIMENTS
where experiment='ec-to-spend-way-finding'
and assignment in ('control','enabled')
--AND VALID_FROM>='2023-02-15'
AND VALID_from<='2023-04-15'
GROUP BY 1,2
qualify assignment_check=1)
where min_valid_from>='2023-03-01'
--and assignment in ('control','enabled')

    )
, experiments as (
select coalesce(a.user_id,b.user_id) as user_id,
a.assignment as iw_assignment,
b.assignment as wayfinding_assignment,
concat('IW: ',iw_assignment,', WF: ', wayfinding_assignment) as experiment_assignments,
min(a.min_valid_from) as iw_start_ts,
min(b.min_valid_from) as wayfinding_start_ts

from exp_iw a join exp_wayfinding b
on a.user_id=b.user_id
where iw_assignment in ('enabled','control')
and wayfinding_Assignment in ('enabled','control')
--ensure users were assigned tests efficiently

group by 1,2,3,4
)
, first_ec as (
select user_id,
min(DISBURSEMENT_TS) as first_Spend_disbursement_Ts
from
ANALYTIC_DB.DBT_MARTS.FCT_OVERDRAFT_DISBURSEMENT
WHERE disbursement_method='Spending'
group by 1
)
, experiment_cohort as (
select A.user_id,a.experiment_assignments,
greatest(iw_start_ts,wayfinding_start_ts) as test_entry_ts,
b.first_Spend_disbursement_Ts,
abs(datediff('seconds',iw_start_ts,wayfinding_Start_ts)) AS SECONDS_between_test
from experiments a join first_ec b
on a.user_id=b.user_id
and first_Spend_disbursement_Ts>test_entry_ts
and SECONDS_between_test<5
)
,banking as (
select user_id,
min(case when is_spend_txn=1 then transaction_ts end) as first_spend_ts,
min(transaction_ts) as first_banking_Ts,
MIN(case when transaction_amount<0 then transaction_ts END) AS first_outgoing_ts
from ANALYTIC_DB.DBT_MARTS.FCT_TRANSACTIONS
group by 1
)
, banking_user as (select user_id,
min(FIRST_CARD_ACTIVATED_DS) as card_activated_ts,
min(FIRST_VIRTUAL_SPEND_AT) as first_virtual_spend_at
FROM
ANALYTIC_DB.DBT_MARTS.FCT_DAVE_BANKING_USER
group by 1)
, banking_Base as (
select a.*,
b.first_spend_ts,
datediff('day',first_Spend_disbursement_Ts,first_spend_ts) as days_until_first_spend,
datediff('day',first_Spend_disbursement_Ts,first_outgoing_ts) as days_until_first_outgoing,
datediff('day',first_Spend_disbursement_Ts,card_activated_ts) as days_until_card_activated,
datediff('day',first_Spend_disbursement_Ts,first_virtual_spend_at) as days_until_first_virtual,
case when first_banking_ts is null then 'No Banking Txn'
wheN first_Spend_disbursement_Ts::date<=first_banking_Ts::date then 'EC First'
else 'Bank first' end as first_transaction
from experiment_cohort a left join banking b
on a.user_id=b.user_id
LEFT join banking_user c
on a.user_id=c.user_id
)

SELECT EXPERIMENT_ASSIGNMENTS,
first_transaction,
COUNT(DISTINCT USER_ID) as users
,COUNT(DISTINCT case when days_until_first_spend is not null and days_until_first_spend<=1 then user_id end) as d1_spender,
COUNT(distinct case when days_until_first_spend is not null and days_until_first_spend<=1 then user_id end) as d7_spender,
COUNT(DISTINCT case when days_until_first_outgoing is not null and days_until_first_outgoing<=7 then user_id end) as d7_outgoing,
COUNT(DISTINCT case when days_until_first_outgoing is not null and days_until_first_outgoing<=1 then user_id end) as d1_outgoing
from banking_Base
GROUP BY 1,2;