{{ config(
    materialized='view',
    tags = ['core_underwriting']
) }}

-- This table is to compare the payment date and due date in utc timezone, but it is not 100% accurate.
-- Comparison of the utc due date with the utc payment date may shift some evening payments to the next day.
-- Converting the payment date to the pst timezone may shift some early morning payments after the due date one day earlier.
-- Neither utc nor converting to pst will be 100% accurate, but we chose to compare utc here to make our logic more aligned and making-sense.

{% set n_days = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 60, 90, 120, 180] %}


WITH

settlement AS (

    SELECT * FROM {{ ref('fct_overdraft_settlement') }}

),

final AS (

    SELECT
        overdraft_id,
        user_id,
        bank_account_id,
        settlement_due_ds_pst,
        amount_due,

        SUM(settlement_amount)               AS total_settled_amount,
        COUNT(DISTINCT settlement_id)        AS payment_count,

        {% for n_day in n_days %}
        SUM(
            IFF(
                DATEADD(DAY, {{ n_day }}, settlement_due_ds_pst) <= CURRENT_DATE AND
                settlement_ds <= DATEADD(DAY, {{ n_day }}, settlement_due_ds),
                settlement_amount,
                NULL
            )
        ) AS paid_{{ n_day }},
        {% endfor %}

        {% for n_day in n_days %}
        LEAST(
            COALESCE(
                SUM(
                    IFF(settlement_ds <= DATEADD(DAY, {{ n_day }}, settlement_due_ds), settlement_amount, 0)
                )
                , 0
            )
            , amount_due) AS settled_{{ n_day }}_days_after_due,
        {% endfor %}

        DATEDIFF(DAY, settlement_due_ds_pst, CURRENT_DATE) AS days_since_payback

    FROM
        settlement
    GROUP BY
        1, 2, 3, 4, 5, DATEDIFF(DAY, settlement_due_ds_pst, CURRENT_DATE)

)

SELECT * FROM final