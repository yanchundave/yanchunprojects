CREATE OR REPLACE VIEW DBT.DEV_HU_PUBLIC.view_monthly_subscription_2dot0_attempts AS
WITH daily_attempts AS (
    SELECT
        DISTINCT
            CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created)::date AS attempt_dt,
            subscription_charge_id
    FROM DAVE.SUBSCRIPTION.subscription_charge_attempt
    WHERE DELETED IS NULL
        AND _DELETED = false
),

daily_attempts_rnk AS (
    SELECT
        attempt_dt,
        subscription_charge_id,
        row_number() OVER (PARTITION BY subscription_charge_id ORDER BY attempt_dt DESC) AS rnk
    FROM daily_attempts
)

SELECT
    attempts.attempt_dt,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sc.term_started) AS term_started_pt,
    term_started_pt::date AS bill_start_dt, -- bill start date in PT
    scs.code AS collection_status, -- collected, pending_collection, failed_collection,
    IFF(bill_start_dt = attempts.attempt_dt, 1, 0) AS is_attempted_at_bill_day,
    IFF(attempts.rnk = 1 and collection_status = 'collected', 'collected', 'failed') AS attempt_status
FROM daily_attempts_rnk attempts
LEFT JOIN DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE sc ON sc.id = attempts.subscription_charge_id AND sc._deleted = false
LEFT JOIN DAVE.subscription.subscription sub ON sc.subscription_id = sub.id
INNER JOIN DAVE.subscription.tier tier ON sub.tier_id = tier.id AND tier.code = 'one_dollar' -- 'one_dollar' only
LEFT JOIN DAVE.SUBSCRIPTION.subscription_charge_status scs ON sc.subscription_charge_status_id = scs.id
WHERE bill_start_dt <= current_date() -- bill is due
    AND sc.unit_cost > 0 -- not free
;
