---settlement by day
{{ config(
    materialized='view',
    tags = ['loss_arrival']
) }}

-- This view is only getting data since 2022 with disbursement_ds >= '2020-12-31'

-- This table is to compare the payment date and due date in utc timezone, but it is not 100% accurate.
-- Comparison of the utc due date with the utc payment date may shift some evening payments to the next day.
-- Converting the payment date to the pst timezone may shift some early morning payments after the due date one day earlier.
-- Neither utc nor converting to pst will be 100% accurate, but we chose to compare utc here to make our logic more aligned and making-sense.

WITH

payment AS (

    SELECT * FROM {{ ref('fct_payments') }}

),

advance AS (

    SELECT * FROM {{ ref('fct_advances') }}

),

final AS (

    SELECT
        a.advance_id                        AS advance_id,
        a.user_id,
        a.disbursement_ds,
        a.payback_date,
        a.original_payback_date,
        a.amount_due,
        a.advance_amount,
        a.fee,
        a.tip,
        SUM(p.payment_amount)               AS total_settled_amount,
        COUNT(DISTINCT p.payment_id)        AS payment_count,

        {% for n_day in range(15) %}

        LEAST( -- "least" is to ignore the settled $ amount over amount_due
            COALESCE(
                SUM(
                    IFF(TO_DATE(p.payment_ts) <= DATEADD(DAY, {{ n_day }}, a.payback_date), p.payment_amount, 0)
                    )
                , 0)
            , a.amount_due)
            AS settled_{{ n_day }}_days_after_due,

        LEAST( -- "least" is to ignore the settled $ amount over amount_due
            COALESCE(
                SUM(
                    IFF(TO_DATE(p.payment_ts) <= DATEADD(DAY, {{ n_day }}, a.disbursement_ds), p.payment_amount, 0)
                    )
                , 0)
            , a.amount_due)
            AS settled_{{ n_day }}_days_after_disbursement,

        {% endfor %}

    DATEDIFF(DAY, a.payback_date, CURRENT_DATE) AS days_since_payback

    FROM advance AS a
        LEFT JOIN payment AS p ON
            a.advance_id = p.advance_id
    WHERE a.disbursement_ds >= '2020-12-31'
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, DATEDIFF(DAY, a.payback_date, CURRENT_DATE)
)

SELECT * FROM final