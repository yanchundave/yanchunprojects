WITH one_dave_new_members AS (
  SELECT
    *
  FROM
    ANALYTIC_DB.DBT_metrics.one_dave_new_members
),
sub_new AS (
  SELECT
    *
  FROM
    DAVE.subscription.subscription sub
),
sc_charge_new AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.SUBSCRIPTION_CHARGE
),
attempts_new AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.subscription_charge_attempt
),
sc_status_new AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.subscription_charge_status
),
tier_new AS (
  SELECT
    *
  FROM
    DAVE.subscription.tier
),
payment_method AS (
  SELECT
    *
  FROM
    DAVE.SUBSCRIPTION.PAYMENT_METHOD_KIND
),
subscriber_new AS (
  SELECT
    sub.USER_ID,
    sub.ID AS SUBSCRIPTION_ID,
    tier.ID AS tier_id,
    started
  FROM
    sub_new sub
    JOIN tier_new tier ON sub.tier_id = tier.id
  WHERE
    tier.code = 'one_dollar'
    AND sub._DELETED = FALSE
),
sub_charge_new AS (
  SELECT
    DATE_TRUNC(
      'MONTH',
      CONVERT_TIMEZONE(
        'UTC',
        'America/Los_Angeles',
        sc_charge.term_started
      )
    ) AS billing_cycle,
    DATE(
      CONVERT_TIMEZONE(
        'UTC',
        'America/Los_Angeles',
        sc_charge.term_started
      )
    ) AS term_started,
    subscriber.user_id,
    subscriber.started,
    sc_charge.id AS subscription_charge_id,
    sc_status.code AS collect_status,
    payment.kind AS payment_final,
    sc_charge.unit_cost,
    sc_charge._deleted
  FROM
    subscriber_new subscriber
    JOIN sc_charge_new sc_charge ON subscriber.subscription_id = sc_charge.subscription_id
    LEFT JOIN sc_status_new sc_status ON sc_charge.subscription_charge_status_id = sc_status.id
    LEFT JOIN payment_method payment ON sc_charge.payment_method_kind_id = payment.id
  WHERE
    term_started >= date('2022-06-01') --WHERE (sc_charge._DELETED = FALSE or sc_charge._DELETED IS NULL)
    qualify row_number() over (
      PARTITION by billing_cycle,
      user_id
      ORDER BY
        term_started
    ) = 1
),
last_attempt_new AS (
  SELECT
    subscription_charge_id,
    date(
      max(
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', created)
      )
    ) AS last_attempt_date
  FROM
    attempts_new
  GROUP BY
    1
),
sub_combine AS (
  SELECT
    s.billing_cycle,
    s.subscription_charge_id,
    s.user_id,
    s.payment_final AS payment_final,
    s.term_started,
    s.unit_cost,
    s.collect_status,
    la.last_attempt_date,
    CASE
      WHEN s._deleted = TRUE THEN 'deleted'
      WHEN s.subscription_charge_id IS NULL THEN 'not_billed'
      WHEN la.last_attempt_date IS NULL
      AND collect_status IS NULL THEN 'not_attempted'
      WHEN s.collect_status <> 'collected' THEN s.collect_status
      WHEN s.collect_status = 'collected'
        AND s.unit_cost = 0 THEN 'free_succeeded'
      WHEN s.collect_status = 'collected'
        AND s.unit_cost > 0
        AND date_trunc('month', la.last_attempt_date) = s.billing_cycle THEN 'success_samemonth'
      WHEN s.collect_status = 'collected'
        AND s.unit_cost > 0
        AND date_trunc('month', la.last_attempt_date) > s.billing_cycle THEN 'success_nextmonth'
      ELSE 'others'
    END AS collect_status_update
  FROM
    sub_charge_new s
    LEFT JOIN last_attempt_new la ON la.subscription_charge_id = s.subscription_charge_id
    LEFT JOIN one_dave_new_members od ON s.user_id = od.user_id

)
SELECT
  *
FROM
  sub_combine