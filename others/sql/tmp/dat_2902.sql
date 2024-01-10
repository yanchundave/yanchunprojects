with collections as (

    select * from analytic_db.dbt_marts.fct_subscription2_collections
),
with attempts as
(
    select * from analytic_db.dbt_marts.fct_subscription2_attempts
)