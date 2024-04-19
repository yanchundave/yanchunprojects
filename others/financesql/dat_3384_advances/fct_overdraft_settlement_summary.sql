-- This table is to compare the payment date and due date in utc timezone, but it is not 100% accurate.
-- Comparison of the utc due date with the utc payment date may shift some evening payments to the next day.
-- Converting the payment date to the pst timezone may shift some early morning payments after the due date one day earlier.
-- Neither utc nor converting to pst will be 100% accurate, but we chose to compare utc here to make our logic more aligned and making-sense.

{% set n_days = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 60, 90, 120, 180] %}


WITH

disbursement AS (

    SELECT * FROM {{ ref('fct_overdraft_disbursement') }}

),

settlement AS (

    SELECT * FROM {{ ref('fct_overdraft_settlement') }}

),

final AS (

    SELECT
        dis.overdraft_id,
        dis.user_id,
        dis.bank_account_id,
        dis.settlement_due_ds_pst,
        dis.amount_due,
        dis.overdraft_amount AS principal,

        SUM(sett.settlement_amount)               AS total_settled_amount,
        COUNT(DISTINCT sett.settlement_id)        AS payment_count,

        {% for n_day in n_days %}

        SUM(
            IFF(
                DATEADD(DAY, {{ n_day }}, dis.settlement_due_ds_pst) <= CURRENT_DATE AND
                sett.settlement_ds <= DATEADD(DAY, {{ n_day }}, sett.settlement_due_ds),
                sett.settlement_amount,
                NULL
            )
        ) AS paid_{{ n_day }},

        LEAST( -- "least" is to ignore the settled $ amount over amount_due
            COALESCE(
                SUM(
                    IFF(sett.settlement_ds <= DATEADD(DAY, {{ n_day }}, dis.settlement_due_ds), sett.settlement_amount, 0)
                ), 0
            )
            , dis.amount_due) AS settled_{{ n_day }}_days_after_due,

        {% endfor %}

        DATEDIFF(DAY, dis.settlement_due_ds_pst, CURRENT_DATE) AS days_since_payback

    FROM disbursement dis
        LEFT JOIN settlement sett -- left join here to include ALL overdraft transactions
            ON dis.overdraft_id = sett.overdraft_id
    GROUP BY
        1, 2, 3, 4, 5, 6, DATEDIFF(DAY, dis.settlement_due_ds_pst, CURRENT_DATE)

)

SELECT * FROM final