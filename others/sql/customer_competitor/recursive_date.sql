WITH RECURSIVE adv_competitor_ref_date_range AS
(
    SELECT
        CASE WHEN '{{ var("backfill_start_date") }}' = 'default' THEN CURRENT_DATE() - 30
             ELSE CAST('{{ var("backfill_start_date") }}' AS DATE)
        END AS start_date,
        CASE WHEN '{{ var("backfill_end_date") }}' = 'default' THEN CURRENT_DATE() - 1
             ELSE CAST('{{ var("backfill_end_date") }}' AS DATE)
        END AS end_date
),
cte_ref_date (ref_date) AS
(
    SELECT
        start_date AS ref_date
    FROM
        adv_competitor_ref_date_range
    UNION ALL
    SELECT
        ref_date + 1 AS ref_date
    FROM
        cte_ref_date
    CROSS JOIN
        adv_competitor_ref_date_range
    WHERE ref_date < end_date

)
SELECT
    ref_date
FROM cte_ref_date