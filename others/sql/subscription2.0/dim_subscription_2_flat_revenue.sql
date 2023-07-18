-------------------------------------------------------------------------
-- Create a view with only two columns: collection_date and            --
-- revenue_collected for subscription 2.0 product                      --
-------------------------------------------------------------------------
{{ config(
    materialized = 'view',
    tags = ["sub"]
) }}

With subscription_charge AS (
    SELECT * FROM {{ source('dave', 'SUBSCRIPTION_CHARGE') }}
)

, subscription_charge_status AS (
    SELECT * FROM {{ source('dave', 'SUBSCRIPTION_CHARGE_STATUS') }}
)

, subscription_charge_attempt AS (
    SELECT * FROM {{ source('dave', 'SUBSCRIPTION_CHARGE_ATTEMPT') }}
)

, latest_charge_attempt AS (
    SELECT SUBSCRIPTION_CHARGE_ID,
           MAX(created) as latest_date
    FROM subscription_charge_attempt
    GROUP BY 1
)

SELECT
       -- The collection date would be the date on which the successful charge was attempted, but in case if it is missing,it would be replaced with the term start date.
       date(coalesce(lca.latest_date , sc.term_started)) as collection_date,
       -- UNIT_COST value is stored in a decimal less system, so to interpret the data correctly, we are dividing it by 100
       sum(coalesce(sc.UNIT_COST,0)/100) as revenue_collected

FROM subscription_charge sc
LEFT JOIN subscription_charge_status  ss
ON sc.subscription_charge_status_id = ss.id
LEFT JOIN latest_charge_attempt lca
ON sc.id = lca.SUBSCRIPTION_CHARGE_ID
WHERE ss.code = 'collected'
GROUP BY 1
ORDER BY 1