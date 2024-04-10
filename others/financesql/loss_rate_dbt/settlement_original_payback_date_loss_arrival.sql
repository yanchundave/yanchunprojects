{{ config(
    tags = ['loss_arrival'],
) }}

WITH

fct_advance_payback AS (

    SELECT * FROM {{ ref('fct_advance_payback_original_payback_date_loss_arrival') }}

),

fct_overdraft_settlement_summary AS (

    SELECT * FROM {{ ref('fct_overdraft_settlement_summary_original_payback_date_loss_arrival') }}

),

final AS (
    SELECT DISTINCT
        fct_advance_payback.advance_id::VARCHAR AS advance_id,
        user_id,
        disbursement_ds,
        payback_date AS settlement_due_ds,
        amount_due,
        advance_amount,
        tip + fee AS pledged_revenue,
        total_settled_amount,

        {% for n_day in range(361) %}

        settled_{{ n_day }}_days_after_disbursement,
        settled_{{ n_day }}_days_after_due{{"," if not loop.last}}

        {% endfor %}

    FROM fct_advance_payback

    UNION ALL

    SELECT DISTINCT
        fct_overdraft_settlement_summary.overdraft_id::VARCHAR AS advance_id,
        user_id,
        disbursement_ds,
        original_payback_date AS settlement_due_ds,
        amount_due,
        overdraft_amount AS advance_amount,
        tip_amount + fee AS pledged_revenue,
        total_settled_amount,

        {% for n_day in range(361) %}

        settled_{{ n_day }}_days_after_disbursement,
        settled_{{ n_day }}_days_after_due{{"," if not loop.last}}

        {% endfor %}

    FROM fct_overdraft_settlement_summary
)

SELECT * FROM final