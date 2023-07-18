CREATE OR REPLACE VIEW DBT.DEV_HU_PUBLIC.view_monthly_subscription_2dot0 AS
WITH attempts AS (
    SELECT
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created) AS created_pt,
        subscription_charge_id,
        COUNT(*) OVER (PARTITION BY subscription_charge_id) AS attempt_cnt
    FROM DAVE.SUBSCRIPTION.subscription_charge_attempt
    WHERE DELETED IS NULL
        AND _DELETED = false
    QUALIFY row_number() OVER (PARTITION BY subscription_charge_id ORDER BY created DESC) = 1 -- only latest record will be included  without it, other subscription_charge_id is kept
)

SELECT
    sc.subscription_id,
    sub.user_id,
    sc.id,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt,
    term_started_pt::date AS bill_start_dt, -- bill start date in PT
    scs.code AS collection_status, -- collected, pending_collection, failed_collection
    coalesce(attempts.attempt_cnt, 0) AS attempt_cnt,
    attempts.created_pt AS last_attempt_ts,
    attempts.created_pt::date AS last_attempt_dt,
    t.externalprocessor AS payment_processor,
    IFF(bill_start_dt = last_attempt_dt, 1, 0) AS is_attempted_at_bill_day,
    IFF(collection_status = 'collected', last_attempt_dt, NULL) AS collected_dt,
    -- IFF(bill_start_dt = collected_dt, 1, 0) AS is_collected_at_bill_day,
    CASE WHEN collected_dt IS NOT NULL AND bill_start_dt = collected_dt THEN 'collected at bill day'
        WHEN collected_dt IS NOT NULL THEN 'collected later'
        ELSE 'not collect'
        END AS collected_at_bill_day_flag,
    sc.unit_cost,
    t.transactionstatus AS loomis_txn_status
FROM DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE sc
LEFT JOIN DAVE.subscription.subscription sub ON sc.subscription_id = sub.id
INNER JOIN DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar' -- 'one_dollar' only
LEFT JOIN DAVE.SUBSCRIPTION.subscription_charge_status scs ON sc.subscription_charge_status_id = scs.id
LEFT JOIN attempts ON sc.id = attempts.subscription_charge_id
LEFT JOIN dave.loomis.transaction t ON sc.reference_id = t.REFERENCEID
WHERE bill_start_dt <= current_date() -- bill is due
    AND sc.unit_cost > 0 -- not free
    AND sc._deleted = false
;