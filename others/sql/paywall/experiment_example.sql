  SELECT
        user_id,
        event_properties:"subscriptions-paywall-placement" AS test_variant,
        MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time)) AS start_ts_pt
    FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    WHERE EVENT_TYPE = '[Experiment] Assignment'
        AND event_time >= '2023-05-10'
        AND event_properties:"subscriptions-paywall-placement.variant" in ('control', 'treatment')
    GROUP BY 1,2


SELECT

        event_properties:"subscriptions-paywall-placement.variant" AS test_variant,
        count(distinct user_id)

        --MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_time)) AS start_ts_pt
 FROM ANALYTIC_DB.DBT_MARTS.AMPLITUDE_DAO
    WHERE EVENT_TYPE = '[Experiment] Assignment'
        AND event_time >= '2023-05-10'
        and event_properties:"subscriptions-paywall-placement.variant" is not null
group by 1