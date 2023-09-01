select try_to_number(a.user_id) as user_id,
        n.pv_ts,
        case
          when event_type = 'plaid - OPEN' then 's1: BC Att'
          when event_type = 'bank connected' then 's2: BC'
          when event_type = 'plaid bank credentials authorized' then 's2.1: BC Auth'
          when event_type = 'debit card added' then 's3: Card Added'
          when event_type = 'extra cash account created' then 's4: EC Acct Opened'
          when event_type = 'checking account ready' then 's4.1: Checking Acct Opened'
          when event_type in ('extra cash at end approved advance amounts screen loaded', 'extra cash landing screen loaded', 'extra cash disbursement screen loaded') then 's5: EC Apvl'
          when event_type in ('extra cash disbursement finalized', 'advance disbursed', 'overdraft disbursement succeeded') then 's6: EC CVR'
          else event_type
        end as event_step,
        convert_timezone('UTC', 'America/Los_Angeles', a.event_time) as event_ts_pst
 from   analytic_db.dbt_marts.amplitude_dao as a


SELECT
  referenceid, regexp_substr(referenceid, '-(\\d)*')
FROM
  DAVE.LOOMIS.TRANSACTION a
  where referenceid like 'backfill-%' limit 10

-- subscription a b test
--sub2

WITH sub_new as (
    select * from DAVE.subscription.subscription sub
),
sc_charge_new as (
    select * from DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
attempts_new as (
    select * from DAVE.SUBSCRIPTION.subscription_charge_attempt
),
sc_status_new as (
    SELECT * FROM DAVE.SUBSCRIPTION.subscription_charge_status
),
tier_new AS
(
    SELECT * FROM DAVE.subscription.tier
),
payment_method AS
(
    SELECT * FROM DAVE.SUBSCRIPTION.PAYMENT_METHOD_KIND
),
subscriber_new AS (
    SELECT
        sub.USER_ID,
        sub.ID AS SUBSCRIPTION_ID,
        tier.ID as tier_id
    from sub_new sub
    JOIN tier_new tier
    ON sub.tier_id = tier.id
    JOIN sandbox.dev_yyang.sub_cohort4 b
    ON sub.user_id = b.id
    WHERE tier.code = 'one_dollar'
        AND  sub._DELETED = FALSE and b.label='sub2'
),
sub_charge_new AS (
    SELECT
        DATE_TRUNC('MONTH',  CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) AS billing_cycle,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc_charge.term_started)) as term_started,
        subscriber.user_id,
        sc_charge.id AS subscription_charge_id,
        sc_status.code as collect_status,
        payment.kind as payment_final,
        sc_charge.unit_cost
    FROM subscriber_new subscriber
    LEFT JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
    WHERE billing_cycle = DATE('2023-08-01')
),
last_attempt_new as (
    select
        subscription_charge_id,
        date(max(created)) as last_attempt_date
    from attempts_new
    group by 1
),
sub_attempts_new AS (
    SELECT
        s.billing_cycle,
        a.id as subscription_attempt_id,
        s.subscription_charge_id,
        s.user_id,
        a.created,
        DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED)) AS attempt_date,
        DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', a.CREATED))) as monthnum,
        s.payment_final as payment_final,
        s.term_started,
        s.unit_cost,
        s.collect_status,
        payment.kind as payment_attempt,
        la.last_attempt_date
    FROM sub_charge_new s
    LEFT JOIN  attempts_new a on a.subscription_charge_id = s.subscription_charge_id
    LEFT JOIN payment_method payment ON a.payment_method_kind_id = payment.id
    LEFT JOIN last_attempt_new la on la.subscription_charge_id = s.subscription_charge_id
    WHERE a.created >= date('2023-08-16')
    AND a.created <= date('{{ selected_end_date }}')
    order by monthnum, attempt_date, created
),
final_new as (
    select
        sn.id as user_id,
        a.* exclude (user_id)
    from sandbox.dev_yyang.sub_cohort4 sn
    left join sub_attempts_new a
    on sn.id = a.user_id
    where sn.label = 'sub2'
)
select * from final_new

{% form %}

selected_end_date:
  type: date
  default: 2023-09-02

{% endform %}

---legacy

with users as
(
    select
        b.id as user_id,
        a.bill_id,
        a.bill_due_date,
        a.payment_method,
        a.is_collect_succeeded,
        a.billing_cycle
    from  sandbox.dev_yyang.sub_cohort4 b
    LEFT JOIN ANALYTIC_DB.DBT_MARTS.FCT_MONTHLY_SUBSCRIPTION_USERS a
    on a.user_id = b.id
    where a.billing_cycle = '2023-08' and b.label = 'legacy'


),
attempts as (
    select
        id as attempt_id,
        created,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) as created_pst,
        subscription_billing_id,
         parse_json(extra):chargeType as chargeType,
        subscription_payment_id
    from APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_COLLECTION_ATTEMPT
    where CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) >= DATE('2023-08-16')
    and CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created::timestamp_ntz) <= DATE('{{ selected_end_date }}')
),
payments as (
    select
        a.attempt_id,
        a.created_pst,
        a.subscription_billing_id,
        a.chargeType,
        b.status,
        b.external_processor,
        b.id as payment_id,
        case
            when b.id is null then
                case
                    when chargeType like 'debit charge%'
                        OR chargeType like '%forced debit only%' then 'DEBIT'
                    WHEN chargeType like 'bank charge%'
                        or chargeType like 'balance is above%' then 'ACH'
                    ELSE 'OTHER'
                END
            WHEN b.id is not null then
                case
                    WHEN external_processor like 'SYNAPSEPAY' THEN 'ACH'
                    WHEN external_processor like 'TABAPAY' THEN 'DEBIT'
                    ELSE 'other'
                end
            else 'others'
        end as processor

    from attempts a
    left join APPLICATION_DB.TRANSACTIONS_DAVE.SUBSCRIPTION_PAYMENT b
    on a.subscription_payment_id = b.id

),
final as (
    select
      a.user_id,
      a.bill_id,
      a.bill_due_date,
      a.payment_method as payment_final,
      a.billing_cycle,
      b.attempt_id,
      b.created_pst,
      date(b.created_pst) AS attempted_date,
      b.chargeType,
      b.status,
      b.external_processor,
      b.payment_id,
      b.processor as payment_attempt
    from  users a
    left join payments b
    on a.bill_id = b.subscription_billing_id
)
select * from final

{% form %}

selected_end_date:
  type: date
  default: 2023-09-02

{% endform %}