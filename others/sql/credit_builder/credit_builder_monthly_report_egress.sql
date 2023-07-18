with
    fct_overdraft_settlement as (
        select * from analytic_db.dbt_marts.fct_overdraft_settlement_ta

    ),

    fct_overdraft_disbursement as (
        select * from analytic_db.dbt_marts.fct_overdraft_disbursement

    ),

    bank_banking_account as (
        select * from application_db.google_cloud_mysql_bank_banking.account

    ),

    bank_banking_account_type as (
        select * from application_db.google_cloud_mysql_bank_banking.account_type

    ),

    bank_banking_account_status as (
        select * from application_db.google_cloud_mysql_bank_banking.account_status

    ),

    overdraft_account as (select * from overdraft.overdraft_overdraft.account),

    overdraft_overdraft as (select * from overdraft.overdraft_overdraft.overdraft),

    overdraft_overdraft_disbursement as (
        select * from overdraft.overdraft_overdraft.disbursement
    ),

    promo_redemption as (select * from dave.promotions.promo_redemption),

    bank_banking_account_payment_reference as (
        select * from application_db.google_cloud_mysql_bank_banking.payment_reference

    ),

    dave_user as (select * from application_db.google_cloud_mysql_dave.user),

    chargeback_transactions as (
        select * from analytic_db.dbt_marts.chargeback_transactions
    ),

    loomis_transaction as (select * from dave.loomis.transaction),

    fct_advance_approvals as (
        select * from analytic_db.dbt_marts.fct_advance_approvals

    ),

    user_enrollment as (select * from dave.credit_builder.user_enrollment),

    reporting_windows as (
        -- This cte is designated to generate reporting windows
        -- default values are the first and last day of the previous calendar month
        -- can add more months if needed
        select
            dateadd(
                month, -1, date_trunc('Month', current_date())
            ) as reporting_start_day,
            dateadd(day, -1, date_trunc('Month', current_date())) as reporting_end_day
    ),

    prn_unique_account as (select distinct prn from finance.galileo.customer),

    extra_cash_settlement as (

        select
            fct_overdraft_settlement.overdraft_id,
            reporting_windows.reporting_end_day,
            sum(
                iff(
                    fct_overdraft_settlement.settlement_ds_pst
                    <= reporting_windows.reporting_end_day,
                    fct_overdraft_settlement.settlement_amount,
                    0
                )
            ) as settlement_amount,
            max(
                iff(
                    fct_overdraft_settlement.settlement_ds_pst
                    <= reporting_windows.reporting_end_day,
                    fct_overdraft_settlement.settlement_ds_pst,
                    null
                )
            ) as date_of_last_payment,
            sum(
                iff(
                    fct_overdraft_settlement.settlement_ds_pst
                    between reporting_windows.reporting_start_day
                    and reporting_windows.reporting_end_day,
                    fct_overdraft_settlement.settlement_amount,
                    0
                )
            ) as settlement_amount_in_reporting_window,
            sum(
                iff(
                    fct_overdraft_settlement.settlement_ds_pst
                    < reporting_windows.reporting_start_day,
                    fct_overdraft_settlement.settlement_amount,
                    0
                )
            ) as settlement_amount_prev_report
        from fct_overdraft_settlement
        cross join reporting_windows
        group by 1, 2

    ),

    -- extra cash table cross join with reporting_end_day
    extra_cash_disbursement_date as (

        select
            fct_overdraft_disbursement.user_id,
            fct_overdraft_disbursement.overdraft_id,
            fct_overdraft_disbursement.amount_due,
            fct_overdraft_disbursement.disbursement_ds_pst,
            fct_overdraft_disbursement.settlement_due_ds_pst as payback_date,
            reporting_windows.reporting_start_day,
            reporting_windows.reporting_end_day,
            iff(
                fct_overdraft_disbursement.disbursement_ds_pst
                between reporting_windows.reporting_start_day
                and reporting_windows.reporting_end_day,
                fct_overdraft_disbursement.amount_due,
                0
            ) as disbursed_amount_in_reporting_window,  -- including tips and fees
            iff(
                fct_overdraft_disbursement.disbursement_ds_pst
                < reporting_windows.reporting_start_day,
                fct_overdraft_disbursement.amount_due,
                0
            ) as amount_due_prev_report
        from fct_overdraft_disbursement
        cross join reporting_windows
        where
            fct_overdraft_disbursement.disbursement_status = 'COMPLETE'
            and fct_overdraft_disbursement.disbursement_ds_pst
            <= reporting_windows.reporting_end_day

    ),

    extra_cash_disbursement as (

        select
            extra_cash_disbursement_date.user_id,
            extra_cash_disbursement_date.overdraft_id,
            extra_cash_disbursement_date.amount_due,
            extra_cash_disbursement_date.disbursement_ds_pst,
            extra_cash_disbursement_date.payback_date,
            extra_cash_settlement.settlement_amount_in_reporting_window,
            extra_cash_disbursement_date.disbursed_amount_in_reporting_window,
            extra_cash_disbursement_date.reporting_end_day,
            extra_cash_settlement.settlement_amount,
            extra_cash_settlement.date_of_last_payment,
            -- this is the new due date defined for the credit builder project which
            -- is always due_date + 30 days grace period
            dateadd(
                days, 30, extra_cash_disbursement_date.payback_date
            ) as new_due_date,

            extra_cash_disbursement_date.amount_due
                - coalesce(extra_cash_settlement.settlement_amount, 0)
             as remaining_balance,  -- round down
            iff(
                remaining_balance > 0
                and new_due_date <= extra_cash_disbursement_date.reporting_end_day,
                datediff(
                    day, new_due_date, extra_cash_disbursement_date.reporting_end_day
                ),
                null
            ) as past_due_days,

            extra_cash_disbursement_date.amount_due_prev_report
                - coalesce(extra_cash_settlement.settlement_amount_prev_report, 0)
             as remaining_balance_prev_report
        from extra_cash_disbursement_date
        left join
            extra_cash_settlement
            on extra_cash_disbursement_date.overdraft_id
            = extra_cash_settlement.overdraft_id
            and extra_cash_disbursement_date.reporting_end_day
            = extra_cash_settlement.reporting_end_day

    ),

    last_pmt_and_occurrence as (

        select
            oa.extra_cash_account_id,
            ecd.reporting_end_day,
            min(
                iff(
                    ecd.remaining_balance > 0
                    and ecd.new_due_date <= ecd.reporting_end_day,
                    ecd.new_due_date,
                    null
                )
            ) as deliquent_date,
            -- The most recent disbursement date for current account
            max(ecd.disbursement_ds_pst) as last_disbursement_date,
            coalesce(deliquent_date, last_disbursement_date) as date_of_occurrence,
            max(ecd.date_of_last_payment) as date_of_last_payment,
            max(ecd.past_due_days) as past_due_days,
            -- regardless of past due or not
            sum(coalesce(ecd.remaining_balance, 0)) as current_balance,
            sum(
                iff(ecd.past_due_days > 0, ecd.remaining_balance, 0)
            ) as deliquent_balance,  -- past due balance
            sum(
                coalesce(ecd.settlement_amount_in_reporting_window, 0)
            ) as settlement_amount_in_reporting_window,
            sum(
                coalesce(ecd.disbursed_amount_in_reporting_window, 0)
            ) as disbursed_amount_in_reporting_window,
            sum(
                coalesce(ecd.remaining_balance_prev_report, 0)
            ) as remaining_balance_prev_report
        from extra_cash_disbursement as ecd
        left join overdraft_overdraft as oo on ecd.overdraft_id = oo.id
        left join overdraft_account as oa on oo.account_id = oa.id
        group by 1, 2

    ),

    chargeback as (
        select
            reporting_windows.reporting_end_day,
            oa.extra_cash_account_id,
            -- negative value
            sum(chargeback_transactions.exception_settled_amount) as chargeback_amount
        from chargeback_transactions
        left join
            loomis_transaction as loomis_transaction
            on chargeback_transactions.original_transaction_id
            = loomis_transaction.externalid
        left join
            fct_overdraft_settlement as s
            on loomis_transaction.referenceid = s.settlement_id
        left join overdraft_overdraft as oo on s.overdraft_id = oo.id
        left join overdraft_account as oa on oo.account_id = oa.id
        cross join reporting_windows
        where
            chargeback_transactions.mid in ('0006', '0008')  -- extra cash
            -- confirmed chargeback
            and chargeback_transactions.dave_internal_cb_status = 'CHARGEBACK - LOSS'
            and chargeback_transactions.status_date
            between reporting_windows.reporting_start_day
            and reporting_windows.reporting_end_day
        group by 1, 2
    ),

    extra_cash_accounts as (
        select
            a.dave_user_id as user_id,
            a.id as extra_cash_account_id,
            c.prn as customer_account_number,  -- hashed account number,
            pr.account_number as test_account_number,
            dave_user.fraud,

            -- suspended accounts are considered as closed, confirmed by Risk team
            dir.reporting_end_day,
            date(
                convert_timezone('UTC', 'America/Los_Angeles', a.created::timestamp_ntz)
            ) as created_ds_pt,
            date(
                convert_timezone(
                    'UTC', 'America/Los_Angeles', oa.closed_at::timestamp_ntz
                )
            ) as closed_ds_pt,
            date(
                convert_timezone('UTC', 'America/Los_Angeles', a.updated::timestamp_ntz)
            ) as updated_ds_pt,  -- only closed before reporting

            iff(
                acs.code in ('closed', 'suspended'),
                coalesce(closed_ds_pt, updated_ds_pt),
                null
            ) as closed_ds_pt_updated,
            iff(
                closed_ds_pt_updated <= dir.reporting_end_day,
                closed_ds_pt_updated,
                null
            ) as closed_ds_pt_final

        from bank_banking_account as a
        inner join
            bank_banking_account_type as att
            on a.account_type_id = att.id
            and att.name = 'Extra Cash Account'
        left join bank_banking_account_status as acs on a.account_status_id = acs.id
        left join
            bank_banking_account_payment_reference as pr
            on a.id = pr.is_primary_for_account_id
        left join
            prn_unique_account as c
            on application_db.google_cloud_mysql_bank_banking.banking_hash(c.prn)
            = pr.account_number
        left join overdraft_account as oa on a.id = oa.extra_cash_account_id
        left join dave_user on a.dave_user_id = dave_user.id
        cross join reporting_windows as dir
        where
            -- accounts only open before the last day of the reporting period
            created_ds_pt <= dir.reporting_end_day
            and not a._fivetran_deleted
            and not att._fivetran_deleted
            and not acs._fivetran_deleted
            and pr.is_primary_for_account_id is not null
    ),

    boost_amt as (
        select
            dis.overdraft_id,
            overdraft.approval_id,
            json_extract_path_text(pp.snapshot, 'data.redemptionAmount') as boost_amount
        from overdraft_overdraft_disbursement as dis
        inner join overdraft_overdraft as overdraft on dis.overdraft_id = overdraft.id
        inner join promo_redemption as pp on dis.promo_id = lower(pp.reference_id)
        where dis.promo_id is not null
    ),

    all_extra_cash_approvals as (
        select distinct
            fa.user_id,
            dir.reporting_end_day,
            date_trunc('MONTH', fa.requested_ds_pst) as requested_month,
            max(fa.max_approved_amount + coalesce(boost.boost_amount, 0)) over (
                partition by requested_month, fa.user_id
            ) as highest_credit_of_month
        from fct_advance_approvals as fa
        left join boost_amt as boost on fa.advance_approval_id = boost.approval_id
        cross join reporting_windows as dir
        where
            fa.product = 'Extra Cash'
            and fa.max_approved_amount is not null
            and fa.max_approved_amount != 0
            and fa.requested_ds_pst <= dir.reporting_end_day
    ),

    credit_all_time_high as (
        select
            user_id, reporting_end_day, max(highest_credit_of_month) as highest_credit
        from all_extra_cash_approvals
        group by 1, 2
    ),

    credit_last_month_high as (
        select user_id, reporting_end_day, highest_credit_of_month as credit_limit
        from all_extra_cash_approvals
        qualify
            row_number() over (
                partition by user_id, reporting_end_day order by requested_month desc
            )
            = 1
    ),

    accounts_enriched as (
        select
            ec.user_id as user_id,
            ec.extra_cash_account_id,
            ec.customer_account_number,
            ec.test_account_number,
            ec.created_ds_pt,
            lpo.date_of_occurrence,
            lpo.date_of_last_payment,
            ec.closed_ds_pt_final,
            lpo.past_due_days,
            lpo.current_balance,
            lpo.deliquent_balance,
            clm.credit_limit,
            cat.highest_credit,  -- chargeback_amount is negative

            -- status assuming the account is not closed
            ec.reporting_end_day,

            -- final account status with consideration of closed
            coalesce(lpo.remaining_balance_prev_report, 0)
            -- previous balance + newly borrowed money in this reporting window
            + coalesce(lpo.disbursed_amount_in_reporting_window, 0) as monthly_payment,

            to_number(
                coalesce(lpo.settlement_amount_in_reporting_window, 0)
                + coalesce(chargeback.chargeback_amount, 0)
            ) as actual_payment,

            case
                -- has past due overdraft
                when lpo.past_due_days is not null and lpo.past_due_days > 0
                then
                    case
                        when lpo.past_due_days <= 30
                        then '71'
                        when lpo.past_due_days <= 60
                        then '78'
                        when lpo.past_due_days <= 90
                        then '80'
                        when lpo.past_due_days <= 120
                        then '82'
                        else '97'  -- over 120 days
                    end
                else '11'
            end as status_if_not_closed,
            case
                when
                    ec.closed_ds_pt_final is not null
                    -- account is closed status as of reporting_end_date
                    and ec.closed_ds_pt_final <= ec.reporting_end_day
                then
                    case
                        when ec.fraud = false  -- not due to fraud
                        then
                            case
                                when
                                    datediff(
                                        day, ec.created_ds_pt, ec.closed_ds_pt_final
                                    )
                                    <= 14  -- closed within 14 days since open
                                then
                                    case
                                        when
                                            date_trunc(
                                                'Month',
                                                date(ec.closed_ds_pt_final)
                                            ) = date_trunc(
                                                'Month', date(ec.created_ds_pt)
                                            )  -- closed and open in the same month
                                        then null
                                        else  -- closed and open in the different month
                                            iff(
                                                date_trunc(
                                                    'Month',
                                                    date(ec.closed_ds_pt_final)
                                                ) = date_trunc(
                                                    'Month', date(ec.reporting_end_day)
                                                ),
                                                'DA',
                                                null
                                            )  -- closed in reporting month
                                    end
                                when
                                    datediff(
                                        day, ec.closed_ds_pt_final, ec.reporting_end_day
                                    )
                                    -- closed for less than or equal 60  days
                                    <= 60
                                then status_if_not_closed
                                else  -- closed more than 60  days
                                    iff(
                                        datediff(
                                            day,
                                            ec.closed_ds_pt_final,
                                            ec.reporting_end_day
                                        )
                                        <= 150,
                                        '13',
                                        null
                                    )
                            end
                        else  -- closed due to fraud
                            iff(
                                date_trunc('Month', date(ec.closed_ds_pt_final))
                                = date_trunc('Month', date(ec.reporting_end_day)),
                                'DF',
                                null
                            )  -- report DF in the closed month
                    end
                else status_if_not_closed  -- not closed
            end as account_status,

            case
                status_if_not_closed
                when '11'
                then '0'
                when '71'
                then '1'
                when '78'
                then '2'
                when '80'
                then '3'
                when '82'
                then '4'
                when '97'
                then 'L'
            end as payment_rating_if_closed
        from extra_cash_accounts as ec
        left join
            last_pmt_and_occurrence as lpo
            on ec.extra_cash_account_id = lpo.extra_cash_account_id
            and ec.reporting_end_day = lpo.reporting_end_day
        left join
            chargeback
            on ec.extra_cash_account_id = chargeback.extra_cash_account_id
            and ec.reporting_end_day = chargeback.reporting_end_day
        left join
            credit_last_month_high as clm
            on ec.user_id = clm.user_id
            and ec.reporting_end_day = clm.reporting_end_day
        left join
            credit_all_time_high as cat
            on ec.user_id = cat.user_id
            and ec.reporting_end_day = cat.reporting_end_day
        inner join
            user_enrollment
            on ec.user_id = user_enrollment.dave_user_id
            and user_enrollment.is_legacy_cb = false
        where
            -- NULL will be filtered out
            account_status is not null and cat.highest_credit > 0
    ),

    final as (
        select
            user_id,
            customer_account_number,
            test_account_number,
            'C' as portfolio_type,
            15 as account_type,
            created_ds_pt as date_open,
            account_status as account_status,
            credit_limit,
            highest_credit,
            'P' as terms_frequency,
            '001' as terms,
            reporting_end_day as date_of_account_information,
            date(date_of_occurrence) as date_of_occurrence,
            date(date_of_last_payment) as date_of_last_payment,
            iff(
                account_status in ('13', 'DA', 'DF'), closed_ds_pt_final, null
            ) as date_closed,
            iff(
                account_status in ('13', 'DA', 'DF'), payment_rating_if_closed, null
            ) as payment_rating,
            iff(
                account_status in ('13', 'DA', 'DF'), 'XA', null
            ) as compliance_condition_code,  -- round down to whole amount
            -- cannot be negative
            to_number(coalesce(current_balance, 0)) as current_balance,
            to_number(coalesce(deliquent_balance, 0)) as amount_past_due,
            to_number(monthly_payment) as monthly_payment,
            to_number(iff(actual_payment > 0, actual_payment, 0)) as actual_payment,
            iff(
                account_status = '97', current_balance, null
            ) as original_charge_off_amount
        from accounts_enriched
    )

select *
from final