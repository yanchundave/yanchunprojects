--Basic datasets for dashboard https://app.mode.com/dave_saves/reports/b69daf2c7c48

WITH
BANK_ACCOUNT AS (
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.BANK_ACCOUNT
),

INSTITUTION AS (
    SELECT * FROM APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.INSTITUTION
),

disbursement AS (
    SELECT
        d.OVERDRAFT_ID,
        date(o.settlement_date) as settlement_date,
        -- ,COALESCE(o.service_fee_amount, 0) as service_fee_amount --
        a.dave_user_id as user_id,
        date(o.created) as created_date,
        (COALESCE(d.amount,0)) as disbursement_amount,
        (COALESCE(d.express_fee,0)) as express_fee,
        (COALESCE(TOTAL_DISBURSED_AMOUNT, 0)) as transferred_amount,
        (COALESCE(o.service_fee_amount, 0)) as service_fee_amount,
        CASE
            WHEN TRY_CAST(SPLIT_PART(Disbursement_METHOD_LOOMIS_ID,':',2) AS integer) is not null
                THEN TRY_CAST(SPLIT_PART(Disbursement_METHOD_LOOMIS_ID,':',2) AS integer)
            ELSE TRY_CAST(PM.LINKEDBANKACCOUNTID as integer)
        end AS BANK_ACCOUNT_ID,
        PM.LINKEDBANKACCOUNTID
    FROM
    OVERDRAFT.OVERDRAFT_OVERDRAFT.DISBURSEMENT d JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.OVERDRAFT o
        ON d.overdraft_ID=o.id
    JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.ACCOUNT a
        ON o.account_id = a.id
    LEFT JOIN LOOMIS_DB.LOOMIS.PAYMENT_METHOD PM
        ON d.Disbursement_METHOD_LOOMIS_ID=PM.paymentmethodid
    WHERE d.DISBURSEMENT_STATUS_ID = 2
),

adjustment AS (
    SELECT
        overdraft_id,
        sum(AMOUNT) as adjust_amount
    from OVERDRAFT.OVERDRAFT_OVERDRAFT.ADJUSTMENT
    group by 1
),

tip_adj AS (
    SELECT
        overdraft_id,
        sum(AMOUNT) as tip_amount
    FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.TIP_ADJUSTMENT
    group by 1
),

settlement as (
    SELECT
        SETTLEMENT.overdraft_id
        , SUM(SETTLEMENT.amount) as total_settlement
        , count (distinct SETTLEMENT.created) as num_settlement
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date) then SETTLEMENT.amount end) as settlement_ontime
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date) then SETTLEMENT.created end) as num_settlement_ontime
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+1 then SETTLEMENT.amount end) as settlement_1
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+1  then SETTLEMENT.created end) as num_settlement_1
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+2 then SETTLEMENT.amount end) as settlement_2
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+2  then SETTLEMENT.created end) as num_settlement_2
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+3 then SETTLEMENT.amount end) as settlement_3
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+3  then SETTLEMENT.created end) as num_settlement_3
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+4 then SETTLEMENT.amount end) as settlement_4
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+4  then SETTLEMENT.created end) as num_settlement_4
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+5 then SETTLEMENT.amount end) as settlement_5
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+5  then SETTLEMENT.created end) as num_settlement_5
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+6 then SETTLEMENT.amount end) as settlement_6
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+6  then SETTLEMENT.created end) as num_settlement_6
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+7 then SETTLEMENT.amount end) as settlement_7
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+7   then SETTLEMENT.created end) as num_settlement_7
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+14 then SETTLEMENT.amount end) as settlement_14
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+14   then SETTLEMENT.created end) as num_settlement_14
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+30 then SETTLEMENT.amount end) as settlement_30
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+30   then SETTLEMENT.created end) as num_settlement_30
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+60 then SETTLEMENT.amount end) as settlement_60
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+60   then SETTLEMENT.created end) as num_settlement_60
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+90 then SETTLEMENT.amount end) as settlement_90
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+90   then SETTLEMENT.created end) as num_settlement_90
        , sum(case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+120 then SETTLEMENT.amount end) as settlement_120
        , count (distinct case when date(SETTLEMENT.created) <= date(overdraft.settlement_date)+120   then SETTLEMENT.created end) as num_settlement_120
        , sum(case when date(SETTLEMENT.created) between date(overdraft.settlement_date)+1 and date(overdraft.settlement_date)+30 then SETTLEMENT.amount end) as settlement_1_to_30
        , count (distinct case when date(SETTLEMENT.created) between date(overdraft.settlement_date)+1 and date(overdraft.settlement_date)+30   then SETTLEMENT.created end) as num_settlement_1_to_30
        , sum(case when date(SETTLEMENT.created)  between date(overdraft.settlement_date)+31  and date(overdraft.settlement_date)+60 then SETTLEMENT.amount end) as settlement_31_to_60
        , count (distinct case when date(SETTLEMENT.created) between date(overdraft.settlement_date)+31  and date(overdraft.settlement_date)+60   then SETTLEMENT.created end) as num_settlement_31_to_60
        , sum(case when date(SETTLEMENT.created) > date(overdraft.settlement_date) then SETTLEMENT.amount end) as settlement_after_duedate

    FROM  OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT SETTLEMENT
    JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.OVERDRAFT OVERDRAFT
        ON SETTLEMENT.overdraft_id = OVERDRAFT.id
    WHERE SETTLEMENT.SETTLEMENT_STATUS_ID = 2
    GROUP BY 1
),

amounts_v1 AS (
    SELECT
        disbursement.overdraft_id
        , disbursement.USER_ID
        , created_date
        , disbursement.settlement_date
        , disbursement.disbursement_amount as disbursed_amount
        , disbursement.transferred_amount
        , disbursement.express_fee as express_fee
        , disbursement.service_fee_amount
        , COALESCE(tip_adj.tip_amount, 0) as tip_amount
        , BANK_ACCOUNT_ID
        , disbursement.disbursement_amount + disbursement.express_fee + disbursement.service_fee_amount + COALESCE(tip_adj.tip_amount, 0)
            - COALESCE(adjustment.adjust_amount, 0) as total_due
        , COALESCE(settlement.total_settlement, 0) as total_settlement
        , COALESCE(settlement.settlement_ontime, 0) as settlement_ontime
        , COALESCE(settlement.settlement_1, 0)  as settlement_1
        , COALESCE(settlement.settlement_2, 0)  as settlement_2
        , COALESCE(settlement.settlement_3, 0)  as settlement_3
        , COALESCE(settlement.settlement_4, 0)  as settlement_4
        , COALESCE(settlement.settlement_5, 0)  as settlement_5
        , COALESCE(settlement.settlement_6, 0)  as settlement_6
        , COALESCE(settlement.settlement_7, 0)  as settlement_7
        , COALESCE(settlement.settlement_14, 0)  as settlement_14
        , COALESCE(settlement.settlement_30, 0)  as settlement_30
        , COALESCE(settlement.settlement_60, 0) as settlement_60
        , COALESCE(settlement.settlement_90, 0) as settlement_90
        , COALESCE(settlement.settlement_120, 0) as settlement_120
        , COALESCE(settlement.settlement_1_to_30, 0)  as settlement_1_to_30
        , COALESCE(settlement.settlement_31_to_60, 0) as settlement_31_to_60
        , COALESCE(settlement.settlement_after_duedate, 0) as settlement_after_duedate
        , COALESCE(settlement.num_settlement, 0) as num_settlement
        , COALESCE(settlement.num_settlement_ontime, 0) as num_settlement_ontime
        , COALESCE(settlement.num_settlement_1, 0)  as num_settlement_1
        , COALESCE(settlement.num_settlement_2, 0)  as num_settlement_2
        , COALESCE(settlement.num_settlement_3, 0)  as num_settlement_3
        , COALESCE(settlement.num_settlement_4, 0)  as num_settlement_4
        , COALESCE(settlement.num_settlement_5, 0)  as num_settlement_5
        , COALESCE(settlement.num_settlement_6, 0)  as num_settlement_6
        , COALESCE(settlement.num_settlement_7, 0)  as num_settlement_7
        , COALESCE(settlement.num_settlement_14, 0)  as num_settlement_14
        , COALESCE(settlement.num_settlement_30, 0)  as num_settlement_30
        , COALESCE(settlement.num_settlement_60, 0) as num_settlement_60
        , COALESCE(settlement.num_settlement_90, 0) as num_settlement_90
        , COALESCE(settlement.num_settlement_120, 0) as num_settlement_120
        , COALESCE(settlement.num_settlement_1_to_30, 0)  as num_settlement_1_to_30
        , COALESCE(settlement.num_settlement_31_to_60, 0) as num_settlement_31_to_60
        , COALESCE(adjustment.adjust_amount, 0) as adjustment_amount

        , CASE
                WHEN (COALESCE(settlement.total_settlement, 0) + COALESCE(adjustment.adjust_amount, 0)) >=
                        (disbursement.disbursement_amount + disbursement.express_fee + disbursement.service_fee_amount + COALESCE(tip_adj.tip_amount, 0) )
                    THEN 'no'
                ELSE 'yes'
            END AS outstanding_status

        , CASE
                WHEN (COALESCE(settlement.settlement_ontime, 0) + COALESCE(adjustment.adjust_amount, 0)) >=
                    (disbursement.disbursement_amount + disbursement.express_fee + disbursement.service_fee_amount + COALESCE(tip_adj.tip_amount, 0))
                    THEN '1: paid_off_on_time'
                WHEN (COALESCE(settlement.settlement_30, 0) + COALESCE(adjustment.adjust_amount, 0)) >=
                    (disbursement.disbursement_amount + disbursement.express_fee + disbursement.service_fee_amount +
                    COALESCE(tip_adj.tip_amount, 0))
                    THEN '2: paid_off_1_to_30days'
                WHEN (COALESCE(settlement.settlement_60, 0) + COALESCE(adjustment.adjust_amount, 0)) >=
                    (disbursement.disbursement_amount + disbursement.express_fee + disbursement.service_fee_amount +
                    COALESCE(tip_adj.tip_amount, 0))
                    THEN '3: paid_off_31_to_60days'
                ELSE '4: did_not_pay_off_in_60_days'
            END AS fully_paid_on_time_flg

        -- add information for # of payment - single vs multiple for fully paid on-time-population --
        , CASE
                WHEN disbursed_amount<= 25 then '1: <=25'
                WHEN disbursed_amount <= 50 then '2: >25-50'
                WHEN disbursed_amount<= 75 then '3: >50-75'
                WHEN disbursed_amount<=100 then '4: >75-100'
                WHEN disbursed_amount>100 then '5: >100'
            END AS disbursed_amount_category

        -- add information on how many days from settlement_date --
        ,datediff(day,date(settlement_date), current_date) as num_days_today_from_due_date
        , CASE
                WHEN datediff(day,date(settlement_date), current_date)  =0 then '1: on_due_date'
                when datediff(day,date(settlement_date), current_date)  <=30 then '2: 1 to 30 days_due'
                when datediff(day,date(settlement_date), current_date)  <=60 then '3: 31 to 60 days_due'
                when datediff(day,date(settlement_date), current_date)  >60 then '4: >60 days_due'
                ELSE '6_others'
            END AS num_days_from_due_date_grp
    FROM disbursement LEFT JOIN tip_adj
        ON disbursement.overdraft_id = tip_adj.overdraft_id
    LEFT JOIN settlement
        ON disbursement.overdraft_id = settlement.overdraft_id
    LEFT JOIN adjustment
        ON disbursement.overdraft_id = adjustment.overdraft_id

),

amounts AS (
    SELECT
        a.*
        , CASE
                 WHEN num_settlement_ontime=1 and fully_paid_on_time_flg='1: paid_off_on_time' then 1
                 else 0
            END AS one_single_full_pay_flag

        , CASE
                WHEN outstanding_status='no' then '1: fully_paid_now'
                when outstanding_status='yes' and num_days_from_due_date_grp='2: 1 to 30 days_due' then '2: outstanding_1-30_days_past_due'
                when outstanding_status='yes' and num_days_from_due_date_grp='3: 31 to 60 days_due' then '3: outstanding_31-60_days_past_due'
                when outstanding_status='yes' and num_days_from_due_date_grp='4: >60 days_due' then '4: outstanding_>60_days_past_due'
                else '5_others'
            END AS past_due_status_now

    FROM amounts_v1 a
),

ACH_return AS (
    SELECT
        a.overdraft_id,
        count(CASE WHEN transactionstatus not in ('UNKNOWN','PENDING','CANCELED') THEN transactionid END ) as all_ach_payments,
        count(CASE WHEN transactionstatus =  'RETURNED' OR statusCode like 'R%' THEN transactionid END ) as returned_ach_payments
    FROM amounts a LEFT JOIN OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT S
        ON a.overdraft_id=s.overdraft_id
    LEFT JOIN LOOMIS_DB.LOOMIS.TRANSACTION t
        ON s.ID = t.REFERENCEID
    WHERE transactionSource = 'ACH'
        AND EXTERNALPROCESSOR = 'GALILEO'
    GROUP BY 1
),

   -- add Institution name for Disbursement bank--
bank_name AS (
    SELECT
        a.OVERDRAFT_ID
        , ins.DISPLAY_NAME
        , CASE WHEN ins.DISPLAY_NAME is null THEN 'blank'
                WHEN ins.DISPLAY_NAME like 'Chime%' THEN 'CHIME'
                WHEN ins.DISPLAY_NAME like 'Varo%' or ins.DISPLAY_NAME ='Albert' or ins.DISPLAY_NAME ='Step'   or ins.DISPLAY_NAME like 'Go%Bank%' THEN 'other neo bank'
                ELSE 'traiditonal'
            END AS bank_category
    FROM amounts a LEFT JOIN BANK_ACCOUNT  ba
        ON a.BANK_ACCOUNT_ID=ba.ID
    LEFT JOIN INSTITUTION ins
        ON ba.INSTITUTION_ID= ins.ID
    ORDER BY ins.DISPLAY_NAME
),

combined AS (
    SELECT
        a.*
        , returned_ach_payments
        , all_ach_payments
        , bank_category
    FROM amounts a LEFT JOIN ACH_return b
        ON a.overdraft_id = b.overdraft_id
    LEFT JOIN bank_name bn
        ON a.overdraft_id=bn.overdraft_id
),

final AS (
    SELECT
        'extra cash' as product
        ,overdraft_id
        ,USER_ID
        ,created_date
        ,settlement_date as due_date
        ,disbursed_amount
        ,transferred_amount
        ,tip_amount
        ,express_fee
        ,service_fee_amount
        ,bank_category
        ,total_due
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
        ,IFF(settlement_ontime >=total_due,1,0) as payoff_ontime_flg
        ,IFF(settlement_1 >=total_due,1,0) as payoff_1day_after_due_flg
        ,IFF(settlement_2 >=total_due,1,0) as payoff_2day_after_due_flg
        ,IFF(settlement_3 >=total_due,1,0) as payoff_3day_after_due_flg
        ,IFF(settlement_4 >=total_due,1,0) as payoff_4day_after_due_flg
        ,IFF(settlement_5 >=total_due,1,0) as payoff_5day_after_due_flg
        ,IFF(settlement_6 >=total_due,1,0) as payoff_6day_after_due_flg
        ,IFF(settlement_7 >=total_due,1,0) as payoff_7day_after_due_flg
        ,IFF(settlement_14>=total_due,1,0) as payoff_14day_after_due_flg
        ,IFF(settlement_30 >=total_due,1,0) as payoff_30day_after_due_flg
        ,IFF(settlement_60 >=total_due,1,0) as payoff_60day_after_due_flg
        ,returned_ach_payments
        ,all_ach_payments
    FROM combined
)
SELECT * FROM final


---EXTRA CASH COLLECTION METRICS

select
    product
    ,date(due_date) as settlement_date
    ,created_date
    ,num_days_from_due_date_grp
    ,disbursed_amount_category
    ,past_due_status_now
    ,fully_paid_on_time_flg
    ,outstanding_status
    ,bank_category
    ,case
        when coalesce(num_settlement,0)=0 then 'a: No payment'
        when coalesce(num_settlement,0)=1 then 'b: 1 payment'
        when coalesce(num_settlement,0)>=2 then 'c: >=2 payments'
        else 'd: others'
    end as num_payments_flg

    --,count(distinct amounts.user_id)  as num_users_disbursed--
    ,count(distinct overdraft_id) as number_advances
    ,sum(total_due) as total_due_$  -- total amt due--
    ,sum(disbursed_amount) as total_disbursed_amount_$
    ,sum(total_settlement) as total_paid_$

    ,sum(settlement_after_duedate) as total_paid_afterdue_$
    ,sum(total_due-total_settlement) as total_outstanding_balance_$
    ,sum(settlement_1_to_30) as total_paid_1_to_30_$
    ,sum(settlement_31_to_60) as total_paid_31_to_60_$
    ,sum(settlement_ontime) as total_paid_ontime_$
    ,sum(settlement_1) as total_paid_1day_$
    ,sum(settlement_2) as total_paid_2day_$
    ,sum(settlement_3) as total_paid_3day_$
    ,sum(settlement_4) as total_paid_4day_$
    ,sum(settlement_5) as total_paid_5day_$
    ,sum(settlement_6) as total_paid_6day_$
    ,sum(settlement_7) as total_paid_7days_$
    ,sum(settlement_14) as total_paid_14days_$
    ,sum(settlement_30) as total_paid_30days_$
    ,sum(settlement_60) as total_paid_60days_$
    ,sum(payoff_ontime_flg) as num_ontime_payoff
    ,sum(payoff_1day_after_due_flg) as num_1day_payoff
    ,sum(payoff_2day_after_due_flg) as num_2day_payoff
    ,sum(payoff_3day_after_due_flg) as num_3day_payoff
    ,sum(payoff_4day_after_due_flg) as num_4day_payoff
    ,sum(payoff_5day_after_due_flg) as num_5day_payoff
    ,sum(payoff_6day_after_due_flg) as num_6day_payoff
    ,sum(payoff_7day_after_due_flg) as num_7day_payoff
    ,sum(payoff_14day_after_due_flg) as num_14day_payoff
    ,sum(payoff_30day_after_due_flg) as num_30day_payoff
    ,sum(payoff_60day_after_due_flg) as num_60day_payoff
    ,sum(returned_ach_payments) as sum_returned_ach_payments
    ,sum(all_ach_payments) as sum_all_ach_payments
    ,count(distinct case when outstanding_status='no' then overdraft_id else null end) as number_fully_paid_now
    ,count(distinct case when outstanding_status='yes' then overdraft_id else null end) as number_outstanding_now

    ,count(distinct case when num_settlement_ontime=1 and payoff_ontime_flg=1 then overdraft_id else null end )
        as num_ontime_one_single_payoff
    ,count(distinct case when num_settlement_1=1 and payoff_1day_after_due_flg=1 then overdraft_id else null end)
        as num_1day_one_single_payoff
    ,count(distinct case when num_settlement_2=1 and payoff_2day_after_due_flg=1 then overdraft_id else null end)
        as num_2day_one_single_payoff
    ,count(distinct case when num_settlement_3=1 and payoff_3day_after_due_flg=1 then overdraft_id else null end)
        as num_3day_one_single_payoff
    ,count(distinct case when num_settlement_4=1 and payoff_4day_after_due_flg=1 then overdraft_id else null end)
        as num_4day_one_single_payoff
    ,count(distinct case when num_settlement_5=1 and payoff_5day_after_due_flg=1 then overdraft_id else null end)
        as num_5day_one_single_payoff
    ,count(distinct case when num_settlement_6=1 and payoff_6day_after_due_flg=1 then overdraft_id else null end)
        as num_6day_one_single_payoff
    ,count(distinct case when num_settlement_7=1 and payoff_7day_after_due_flg=1 then overdraft_id else null end)
        as num_7day_one_single_payoff
    ,count(distinct case when num_settlement_30=1 and payoff_30day_after_due_flg=1 then overdraft_id else null end)
        as num_30day_one_single_payoff
    ,count(distinct case when num_settlement_60=1 and payoff_60day_after_due_flg=1 then overdraft_id else null end)
        as num_60day_one_single_payoff
    ,count(distinct case when num_settlement=1 and outstanding_status='no' then overdraft_id else null end )
         as num_one_single_payoff

 -- # of EC that did not pay off before/on settlement date but paid off within 30 days from settlement date --
    ,count(distinct case when fully_paid_on_time_flg='2: paid_off_1_to_30days' then overdraft_id else null end)
         as num_pay_off_in_1_to_30days
         -- $ of EC that did not pay off before/on settlement date but paid off within 30 days from settlement date --

    ,count(distinct case when fully_paid_on_time_flg<>'1: paid_off_on_time' then overdraft_id else null end)
        as num_not_paidoff_ontime

      --- add more ---
    ,count(distinct case when num_settlement_1_to_30=1 and fully_paid_on_time_flg='2: paid_off_1_to_30days' then overdraft_id else null end)
        as num_one_single_pay_off_1_to_30days

    ,count(distinct case when num_settlement_1_to_30>1 and fully_paid_on_time_flg='2: paid_off_1_to_30days' then overdraft_id else null end)
         as num_multiple_pay_off_1_to_30days

    ,count(distinct case when num_settlement_31_to_60=1 and fully_paid_on_time_flg='2: paid_off_1_to_30days' then overdraft_id else null end)
         as num_one_single_pay_off_31_to_60days

    ,count(distinct case when num_settlement_31_to_60>1 and fully_paid_on_time_flg='3: paid_off_31_to_60days' then overdraft_id else null end)
         as num_multiple_pay_off_31_to_60days

FROM final
WHERE date(due_date)<current_date and date(due_date)>'2021-12-19' and date(created_date)>'2021-12-19'
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,2,3,4,5,6,7,8,9,10