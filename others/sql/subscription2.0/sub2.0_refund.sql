with sub as
(
    select
        sub.id ,
        sub.user_id
    from DAVE.subscription.subscription sub
    inner join DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar'
    where
        started >= date('2023-06-20') and
        started <= date('2023-07-10') and
        sub._DELETED = 'FALSE'
),

sc as
(
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE where _DELETED='FALSE'
),
scupdate as
(
    select
        sub.user_id,
        sub.id as subscription_id,
        sc.id as subscription_charge_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt,
        sc.subscription_charge_status_id
    from sub
    join sc on sub.id = sc.subscription_id
    where term_started_pt >= Date('2023-06-20') and term_started_pt <= Date('2023-07-10')
),


subscription_charge_status AS (

	SELECT * FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE_STATUS
       WHERE _DELETED = 'FALSE'

),

subscription_charge_attempt AS (

	SELECT * FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE_ATTEMPT
       WHERE _DELETED = 'FALSE'

),

subscription_refund AS (

	SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_REFUND
       WHERE _FIVETRAN_DELETED = 'FALSE'

),

reimbursement AS (

	SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.REIMBURSEMENT
       WHERE _FIVETRAN_DELETED = 'FALSE'

),

dashboard_action_log AS (

	SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.DASHBOARD_ACTION_LOG
       WHERE _FIVETRAN_DELETED = 'FALSE'

),

dashboard_action_reason AS (

	SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.DASHBOARD_ACTION_REASON
       WHERE _FIVETRAN_DELETED = 'FALSE'

),

dashboard_action AS (

	SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.DASHBOARD_ACTION
       WHERE _FIVETRAN_DELETED = 'FALSE'

),


subs2_latest_charge_attempt AS (
    SELECT SUBSCRIPTION_CHARGE_ID,
           MAX(created) as latest_date
    FROM subscription_charge_attempt
    GROUP BY 1
),

subsc2_txns as (
SELECT
       -- The collection date would be the date on which the successful charge was attempted, but in case if it is missing,it would be replaced with the term start date.
       date(coalesce(lca.latest_date , sc.term_started)) as payment_created,
       sc.id as subscription_payment_id,
       sc.reference_id,
       coalesce(sc.UNIT_COST,0)/100 as subsc_fee_collected
FROM  sc
LEFT JOIN subscription_charge_status  ss
ON sc.subscription_charge_status_id = ss.id
LEFT JOIN subs2_latest_charge_attempt lca
ON sc.id = lca.SUBSCRIPTION_CHARGE_ID
WHERE ss.code = 'collected'
),

subsc2_refund AS (
    SELECT r.created as refund_created, -- Refund Created
           r.user_id,
           subscription_payment_reference_id,
           r.amount AS subscription_refund,
           r.status AS refund_status,
           r.external_processor AS refund_processor,
           a.code AS refund_reason
    FROM subscription_refund sr
    LEFT JOIN reimbursement r
    ON sr.reimbursement_id = r.id
    LEFT JOIN dashboard_action_log l
    ON r.dashboard_action_log_id = l.id
    LEFT JOIN dashboard_action_reason dr
    ON l.dashboard_action_reason_id = dr.id
    LEFT JOIN dashboard_action a
    ON a.id = dr.dashboard_action_id
    -- For subscription 2.0, we shouldn't expect completed reimbursements to be stuck at PENDING status,
    -- if we do find any weird cases, we should report to eng for investigation.
    WHERE r.status IN ('COMPLETED')
),

subsc2_refund_details as
(select refund_created,
        user_id,
        subscription_refund,
        refund_reason,
        refund_status,
        refund_processor,
        to_varchar(subscription_payment_id) as subscription_payment_id,
        payment_created,
        subsc_fee_collected,
        'subs2' as product
from subsc2_refund
inner join subsc2_txns
on subsc2_txns.reference_id = subsc2_refund.subscription_payment_reference_id
),

generic_refund as
(select r.created as refund_created,
        r.user_id,
        r.amount as subscription_refund,
        a.code as refund_reason,
        r.status as refund_status,
        r.external_processor AS refund_processor,
        -- the predefined null values below are because generic subscription refund can't be tied
        -- back to the original subscription payment or subscription billing cycle
        null as subscription_payment_id,
        null as payment_created,
        null as subsc_fee_collected,
        'generic subs' as product
 from reimbursement r
 left join dashboard_action_log l
 on r.dashboard_action_log_id = l.id
 left join dashboard_action_reason dr
 on l.dashboard_action_reason_id = dr.id
 left join dashboard_action a
 on a.id = dr.dashboard_action_id
     -- There is some weirdness when we disburse refunds to a Dave spending account
     -- that sometimes these transactions don't update from PENDING to COMPLETED; So
     -- we include both 'PENDING' AND 'COMPLETED' status. But there are also other
     -- scenarios that caused the refunds to stuck at 'PENDING' status, which needs further
     -- reconciliation by eng. But the total 'PENDING' refund amount is only ~$600 as of June 2023,
     -- which is not material.
 where r.status IN ('COMPLETED', 'PENDING')
 and a.code = 'create-subscription-reimbursement'
),

final as
(
select *
from subsc2_refund_details
union all
select *
from generic_refund)

select *
from final
join scupdate
on final.user_id = scupdate.user_id and final.subscription_payment_id = scupdate.subscription_charge_id