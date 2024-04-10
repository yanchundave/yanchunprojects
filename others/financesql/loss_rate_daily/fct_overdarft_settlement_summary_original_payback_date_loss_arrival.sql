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

settlement AS (

    SELECT * FROM {{ ref('fct_overdraft_settlement') }}

),

disbursement AS (

    SELECT * FROM {{ ref('fct_overdraft_disbursement') }}

),

final AS (

    SELECT
        dis.overdraft_id,
        dis.user_id,
        dis.bank_account_id,
        dis.disbursement_ds,
        dis.original_payback_date,
        dis.amount_due,
        dis.overdraft_amount,
        dis.express_fee_amount + dis.service_fee_amount     AS fee,
        dis.tip_amount,
        SUM(sett.settlement_amount)                         AS total_settled_amount,
        COUNT(DISTINCT sett.settlement_id)                  AS payment_count,

        {% for n_day in range(361) %}

        LEAST( -- "least" is to ignore the settled $ amount over amount_due
            COALESCE(
                SUM(
                    IFF(sett.settlement_ds <= DATEADD(DAY, {{ n_day }}, dis.original_payback_date), sett.settlement_amount, 0)
                    )
                , 0)
            , dis.amount_due)
            AS settled_{{ n_day }}_days_after_due,

        LEAST( -- "least" is to ignore the settled $ amount over amount_due
            COALESCE(
                SUM(
                    IFF(sett.settlement_ds <= DATEADD(DAY, {{ n_day }}, dis.disbursement_ds), sett.settlement_amount, 0)
                    )
                , 0)
            , dis.amount_due)
            AS settled_{{ n_day }}_days_after_disbursement,

        {% endfor %}

        DATEDIFF(DAY, dis.original_payback_date, CURRENT_DATE) AS days_since_payback

    FROM disbursement dis
        LEFT JOIN settlement sett
            ON dis.overdraft_id = sett.overdraft_id
    WHERE dis.disbursement_ds >= '2020-12-31'
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, DATEDIFF(DAY, dis.original_payback_date, CURRENT_DATE)
)

SELECT * FROM final