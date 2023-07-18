
-------------------------------------------------------------------------
-- Create a view with only two columns: collection_date and            --
-- revenue_collected for subscription 1.0 product                      --
-------------------------------------------------------------------------
{{ config(
    materialized = 'view',
    tags = ["sub"]
) }}

with fct_subscription_payments AS (

	SELECT *
    FROM {{source("dbt_marts", "FCT_SUBSCRIPTION_PAYMENTS")}}

)

SELECT date(subscription_payment_ds) as sub_payment_date
    , sum(subscription_payment_amount) as revenue_collected
FROM fct_subscription_payments
GROUP BY 1
ORDER BY 1