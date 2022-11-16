--Basic datasets for dashboard https://app.mode.com/dave_saves/reports/b69daf2c7c48

WITH payment AS (
    SELECT * FROM APPLICATION_DB.TRANSACTIONS_DAVE.PAYMENT
),

advance AS (
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.ADVANCE
),

advance_tip AS (
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.ADVANCE_TIP
),

BANK_ACCOUNT AS (
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT
),

INSTITUTION AS (
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.INSTITUTION
),

payment_due AS (
    SELECT
        adv.id AS advance_id,
        adv.user_id,
        adv.amount AS disbursed_amount,
        adv.outstanding,
        date(adv.CREATED_DATE) AS created_date,
        date(adv.PAYBACK_DATE) AS PAYBACK_DATE,
            adv.BANK_ACCOUNT_ID,
        COALESCE(adv.fee, 0) AS fee,
        COALESCE(tip.amount,0) AS tip,
        COALESCE(tip.amount,0) + COALESCE(adv.fee, 0) AS addons,
        adv.amount + COALESCE(adv.fee,0) + COALESCE(tip.amount,0) AS total_due
    FROM
        (
            SELECT
                id,
                user_id,
                amount,
                outstanding,
                CREATED_DATE,
                PAYBACK_DATE,
                fee,
                BANK_ACCOUNT_ID
            FROM ADVANCE
            WHERE disbursement_status = 'COMPLETED'
                AND (deleted > current_date OR deleted is NULL)
                AND date(created)<current_date
        ) AS adv
    LEFT JOIN ADVANCE_TIP tip ON adv.id = tip.advance_id
),

payment_made AS (
    SELECT
        a.advance_id AS id
        ,COALESCE(sum(a.amount),0) AS total_settlement
        ,count (DISTINCT a.created) AS num_settlement
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date) THEN a.amount END),0) AS settlement_ontime
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date) THEN a.created END),0) AS num_settlement_ontime
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+1 THEN a.amount END),0) AS settlement_1
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+1  THEN a.created END),0) AS num_settlement_1
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+2 THEN a.amount END),0) AS settlement_2
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+2  THEN a.created END),0) AS num_settlement_2
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+3 THEN a.amount END),0) AS settlement_3
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+3  THEN a.created END),0) AS num_settlement_3
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+4 THEN a.amount END),0) AS settlement_4
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+4  THEN a.created END),0) AS num_settlement_4
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+5 THEN a.amount END),0) AS settlement_5
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+5  THEN a.created END),0) AS num_settlement_5
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+6 THEN a.amount END),0) AS settlement_6
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+6  THEN a.created END),0) AS num_settlement_6
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+7 THEN a.amount END),0) AS settlement_7
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+7   THEN a.created END),0) AS num_settlement_7
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+14 THEN a.amount END),0) AS settlement_14
        ,COALESCE(count(DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+14   THEN a.created END),0) AS num_settlement_14
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+30 THEN a.amount END),0) AS settlement_30
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+30   THEN a.created END),0) AS num_settlement_30
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+60 THEN a.amount END),0) AS settlement_60
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+60   THEN a.created END),0) AS num_settlement_60
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+90 THEN a.amount END),0) AS settlement_90
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+90   THEN a.created END),0) AS num_settlement_90
        ,COALESCE(sum(CASE WHEN date(a.created) <= date(b.payback_date)+120 THEN a.amount END),0) AS settlement_120
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) <= date(b.payback_date)+120   THEN a.created END),0) AS num_settlement_120
        ,COALESCE(sum(CASE WHEN date(a.created) between date(b.payback_date)+1 AND date(b.payback_date)+30 THEN a.amount END),0) AS settlement_1_to_30
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) between date(b.payback_date)+1 AND date(b.payback_date)+30   THEN a.created END),0) AS num_settlement_1_to_30
        ,COALESCE(sum(CASE WHEN date(a.created)  between date(b.payback_date)+31  AND date(b.payback_date)+60 THEN a.amount END),0) AS settlement_31_to_60
        ,COALESCE(count (DISTINCT CASE WHEN date(a.created) between date(b.payback_date)+31  AND date(b.payback_date)+60   THEN a.created END),0) AS num_settlement_31_to_60
        ,COALESCE(sum(CASE WHEN date(a.created) > date(b.payback_date) THEN a.amount END),0) AS settlement_after_duedate
    FROM PAYMENT a
    JOIN advance b ON a.advance_id = b.id
    WHERE a.status = 'COMPLETED'
    GROUP BY 1
),

amounts AS (
    SELECT
        a.*
        ,b.*
        ,CASE
            WHEN disbursed_amount<= 25 THEN '1: <=25'
            WHEN disbursed_amount <= 50 THEN '2: >25-50'
            WHEN disbursed_amount<= 75 THEN '3: >50-75'
            WHEN disbursed_amount<=100 THEN '4: >75-100'
            WHEN disbursed_amount>100 THEN '5: >100'
        END AS disbursed_amount_category

        -- add information on how many days from payback_date --
        ,datediff(day,date(payback_date), current_date) AS num_days_today_from_due_date
        ,CASE
            WHEN datediff(day,date(payback_date), current_date)  =0 THEN '1: on_due_date'
            WHEN datediff(day,date(payback_date), current_date)  <=30 THEN '2: 1 to 30 days_due'
            WHEN datediff(day,date(payback_date), current_date)  <=60 THEN '3: 31 to 60 days_due'
            WHEN datediff(day,date(payback_date), current_date)  >60 THEN '4: >60 days_due'
            ELSE '6_others'
        END AS num_days_from_due_date_grp
        ,CASE WHEN b.total_settlement >= a.total_due THEN 'no' ELSE 'yes' END AS outstanding_status
        ,CASE
            WHEN b.settlement_ontime >=a.total_due THEN '1: paid_off_on_time'
            WHEN b.settlement_30 >= a.total_due THEN '2: paid_off_1_to_30days'
            WHEN b.settlement_60>=a.total_due THEN '3: paid_off_31_to_60days'
            ELSE '4: did_not_pay_off_in_60_days'
        END AS fully_paid_on_time_flg
        ,CASE
            WHEN num_settlement_ontime=1 AND fully_paid_on_time_flg='1: paid_off_on_time' THEN 1
            ELSE 0
        END AS one_single_full_pay_flag
        ,CASE
            WHEN outstanding_status='no' THEN '1: fully_paid_now'
            WHEN outstanding_status='yes' AND num_days_from_due_date_grp='2: 1 to 30 days_due' THEN '2: outstanding_1-30_days_past_due'
            WHEN outstanding_status='yes' AND num_days_from_due_date_grp='3: 31 to 60 days_due' THEN '3: outstanding_31-60_days_past_due'
            WHEN outstanding_status='yes' AND num_days_from_due_date_grp='4: >60 days_due' THEN '4: outstanding_>60_days_past_due'
            ELSE '5_others'
        END AS past_due_status_now
    FROM payment_due a
    LEFT JOIN payment_made b
        ON a.advance_id = b.id
),

ACH_return AS
(
    SELECT
        a.advance_id
        ,COUNT(CASE WHEN status='RETURNED' THEN id END) AS returned_ach_payments
        ,COUNT(CASE WHEN status NOT IN ('CANCELED', 'UNKNOWN', 'PENDING') THEN id END) AS all_ach_payments
    FROM PAYMENT a
    WHERE EXTERNAL_PROCESSOR = 'SYNAPSEPAY'
        AND EXTERNAL_ID is NOT NULL
        AND deleted is null
    GROUP BY 1
),

bank_name AS (
    SELECT
        a.advance_id
        ,ins.DISPLAY_NAME
        ,CASE
            WHEN ins.DISPLAY_NAME is null THEN 'blank'
            WHEN ins.DISPLAY_NAME like 'Chime%' THEN 'CHIME'
            WHEN ins.DISPLAY_NAME like 'Varo%' or ins.DISPLAY_NAME ='Albert' or ins.DISPLAY_NAME ='Step'   or ins.DISPLAY_NAME like 'Go%Bank%' THEN 'other neo bank'
            ELSE 'traiditonal'
        END AS bank_category
    FROM amounts a
    LEFT JOIN BANK_ACCOUNT  ba
        ON a.BANK_ACCOUNT_ID=ba.ID
    LEFT JOIN INSTITUTION ins
        ON ba.INSTITUTION_ID= ins.ID
    ORDER BY ins.DISPLAY_NAME
),

combined AS (
    SELECT
        a.*
        ,returned_ach_payments
        ,all_ach_payments
        ,bank_category
        ,DISPLAY_NAME
    FROM amounts a
    left join ACH_return b
    on a.advance_id = b.advance_id
    left join bank_name AS bn on a.advance_id = bn.advance_id
),

final AS (
    select
        'advance' AS product
        ,advance_id
        ,USER_ID
        ,created_date
        ,payback_date AS due_date
        ,disbursed_amount
        ,outstanding
        ,tip AS tip_amount
        ,fee
        ,total_due
        ,DISPLAY_NAME
        ,bank_category
        ,total_settlement
        ,settlement_ontime
        ,settlement_1
        ,settlement_2
        ,settlement_3
        ,settlement_4
        ,settlement_5
        ,settlement_6
        ,settlement_7
        ,settlement_14
        ,settlement_30
        ,settlement_60
        ,settlement_90
        ,settlement_120
        ,settlement_1_to_30
        ,settlement_31_to_60
        ,settlement_after_duedate
        ,num_settlement
        ,num_settlement_ontime
        ,num_settlement_1
        ,num_settlement_2
        ,num_settlement_3
        ,num_settlement_4
        ,num_settlement_5
        ,num_settlement_6
        ,num_settlement_7
        ,num_settlement_14
        ,num_settlement_30
        ,num_settlement_60
        ,num_settlement_90
        ,num_settlement_120
        ,num_settlement_1_to_30
        ,num_settlement_31_to_60
        ,outstanding_status
        ,fully_paid_on_time_flg
        ,disbursed_amount_category
        ,num_days_from_due_date_grp
        ,one_single_full_pay_flag
        ,past_due_status_now
        ,IFF(settlement_ontime >=total_due,1,0) AS payoff_ontime_flg
        ,IFF(settlement_1 >=total_due,1,0) AS payoff_1day_after_due_flg
        ,IFF(settlement_2 >=total_due,1,0) AS payoff_2day_after_due_flg
        ,IFF(settlement_3 >=total_due,1,0) AS payoff_3day_after_due_flg
        ,IFF(settlement_4 >=total_due,1,0) AS payoff_4day_after_due_flg
        ,IFF(settlement_5 >=total_due,1,0) AS payoff_5day_after_due_flg
        ,IFF(settlement_6 >=total_due,1,0) AS payoff_6day_after_due_flg
        ,IFF(settlement_7 >=total_due,1,0) AS payoff_7day_after_due_flg
        ,IFF(settlement_14>=total_due,1,0) AS payoff_14day_after_due_flg
        ,IFF(settlement_30 >=total_due,1,0) AS payoff_30day_after_due_flg
        ,IFF(settlement_60 >=total_due,1,0) AS payoff_60day_after_due_flg
        ,returned_ach_payments
        ,all_ach_payments
    FROM combined
)
SELECT * FROM final

----Collection KPI Dashboard

SELECT
    product
    ,date(due_date) AS settlement_date
    ,created_date
    ,num_days_from_due_date_grp
    ,disbursed_amount_category
    ,past_due_status_now
    ,fully_paid_on_time_flg
    ,outstanding_status
    ,bank_category
    ,CASE
        WHEN coalesce(num_settlement,0)=0 THEN 'a: No payment'
        WHEN coalesce(num_settlement,0)=1 THEN 'b: 1 payment'
        WHEN coalesce(num_settlement,0)>=2 THEN 'c: >=2 payments'
        ELSE 'd: others'
    END AS num_payments_flg

    --,count(distinct amounts.user_id)  AS num_users_disbursed--
    ,count(DISTINCT advance_id) AS number_advances
    ,sum(total_due) AS total_due_$  -- total amt due--
    ,sum(disbursed_amount) AS total_disbursed_amount_$
    ,sum(total_settlement) AS total_paid_$

    ,sum(settlement_after_duedate) AS total_paid_afterdue_$
    ,sum(total_due-total_settlement) AS total_outstanding_balance_$
    ,sum(settlement_1_to_30) AS total_paid_1_to_30_$
    ,sum(settlement_31_to_60) AS total_paid_31_to_60_$
    ,sum(settlement_ontime) AS total_paid_ontime_$
    ,sum(settlement_1) AS total_paid_1day_$
    ,sum(settlement_2) AS total_paid_2day_$
    ,sum(settlement_3) AS total_paid_3day_$
    ,sum(settlement_4) AS total_paid_4day_$
    ,sum(settlement_5) AS total_paid_5day_$
    ,sum(settlement_6) AS total_paid_6day_$
    ,sum(settlement_7) AS total_paid_7days_$
    ,sum(settlement_14) AS total_paid_14days_$
    ,sum(settlement_30) AS total_paid_30days_$
    ,sum(settlement_60) AS total_paid_60days_$
    ,sum(payoff_ontime_flg) AS num_ontime_payoff
    ,sum(payoff_1day_after_due_flg) AS num_1day_payoff
    ,sum(payoff_2day_after_due_flg) AS num_2day_payoff
    ,sum(payoff_3day_after_due_flg) AS num_3day_payoff
    ,sum(payoff_4day_after_due_flg) AS num_4day_payoff
    ,sum(payoff_5day_after_due_flg) AS num_5day_payoff
    ,sum(payoff_6day_after_due_flg) AS num_6day_payoff
    ,sum(payoff_7day_after_due_flg) AS num_7day_payoff
    ,sum(payoff_14day_after_due_flg) AS num_14day_payoff
    ,sum(payoff_30day_after_due_flg) AS num_30day_payoff
    ,sum(payoff_60day_after_due_flg) AS num_60day_payoff
    ,sum(returned_ach_payments) AS sum_returned_ach_payments
    ,sum(all_ach_payments) AS sum_all_ach_payments
    ,count(DISTINCT CASE WHEN outstanding_status='no' THEN advance_id ELSE null END) AS number_fully_paid_now
    ,count(DISTINCT CASE WHEN outstanding_status='yes' THEN advance_id ELSE null END) AS number_outstanding_now

    ,count(DISTINCT
        CASE
            WHEN num_settlement_ontime=1 AND payoff_ontime_flg=1 THEN advance_id
            ELSE null
        END ) AS num_ontime_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_1=1 AND payoff_1day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_1day_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_2=1 AND payoff_2day_after_due_flg=1 THEN advance_id
            ELSE null END) AS num_2day_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_3=1 AND payoff_3day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_3day_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_4=1 AND payoff_4day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_4day_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_5=1 AND payoff_5day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_5day_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_6=1 AND payoff_6day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_6day_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_7=1 AND payoff_7day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_7day_one_single_payoff

    ,count(DISTINCT
        case
            WHEN num_settlement_30=1 AND payoff_30day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_30day_one_single_payoff

    ,count(DISTINCT
        CASE
            WHEN num_settlement_60=1 AND payoff_60day_after_due_flg=1 THEN advance_id
            ELSE null
        END) AS num_60day_one_single_payoff

    ,count(DISTINCT
        CASE
            WHEN num_settlement=1 AND outstanding_status='no' THEN advance_id
            ELSE null
        END ) AS num_one_single_payoff

    -- # of EC that did not pay off before/on settlement date but paid off within 30 days from settlement date --
    ,count(DISTINCT
        CASE
            WHEN fully_paid_on_time_flg='2: paid_off_1_to_30days' THEN advance_id
            ELSE null
        END) AS num_pay_off_in_1_to_30days

    -- $ of EC that did not pay off before/on settlement date but paid off within 30 days from settlement date --
    ,count(DISTINCT
        CASE
            WHEN fully_paid_on_time_flg<>'1: paid_off_on_time' THEN advance_id
            ELSE null
        END) AS num_not_paidoff_ontime

    --- add more ---
    ,count(DISTINCT
        CASE
            WHEN num_settlement_1_to_30=1 AND fully_paid_on_time_flg='2: paid_off_1_to_30days' THEN advance_id
            ELSE null
        END) AS num_one_single_pay_off_1_to_30days

    ,count(DISTINCT
        CASE
            WHEN num_settlement_1_to_30>1 AND fully_paid_on_time_flg='2: paid_off_1_to_30days' THEN advance_id
            ELSE null
        END) AS num_multiple_pay_off_1_to_30days

    -- NEED CHECK WITH NI ABOUT THE LOGIC. HER SCRIPT WAS paid_off_1_to_30days
    ,count(DISTINCT
        CASE
            WHEN num_settlement_31_to_60=1 AND fully_paid_on_time_flg='3: paid_off_31_to_60days' THEN advance_id
            ELSE null
        END) AS num_one_single_pay_off_31_to_60days

    ,count(DISTINCT
        CASE
            WHEN num_settlement_31_to_60>1 AND fully_paid_on_time_flg='3: paid_off_31_to_60days' THEN advance_id
            ELSE null
        END) AS num_multiple_pay_off_31_to_60days
FROM final --dr
WHERE date(due_date)<current_date AND date(due_date)>='2021-01-01'
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,2,3,4,5,6,7,8,9,10