--drop table if exists sandbox.dev_yyang.da_ltv_predict;
create table sandbox.dev_yyang.da_ltv_predict as
with fct_advance_approvals as
(
    select * from ANALYTIC_DB.DBT_marts.fct_advance_approvals
),

fct_advances as
(
    select * from ANALYTIC_DB.DBT_marts.fct_advances
),

fct_overdraft_disbursement  as
(
    select * from ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement
),

disbursements as (

    select * from ANALYTIC_DB.DBT_marts.disbursements
),

settlements as (

    select * from ANALYTIC_DB.DBT_marts.settlements
),

settlement_details as (
    select * from OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT
),

settlement_status AS (

    SELECT * FROM OVERDRAFT.OVERDRAFT_OVERDRAFT.SETTLEMENT_STATUS

),
payment_details as (
    select * from ANALYTIC_DB.DBT_marts.fct_payments
),

new_user_attribution as (

    select * , rank() over (partition by user_id order by network) as rnk from ANALYTIC_DB.DBT_marts.new_user_attribution
    qualify rnk = 1

),

ltv_bankconnection as (

    select *,
        rank() over (partition by user_id order by has_valid_credentials, bank_category desc) as ranknum
    from ANALYTIC_DB.DBT_metrics.ltv_bankconnection
    qualify ranknum = 1
),

user_income as (
    select
        user_id,
        sum(last_30d_income) as monthly_income
    from
        (
            select *
            from DAVE.INCOME_DETECTION_INCOME_DETECTION.PAYROLL_ASSESSMENT
            where num_valid_payroll_groups >= 1
                and _fivetran_deleted = 'false'
            qualify row_number() over(partition by user_id, bank_account_id
            order by assessment_date_time desc) = 1
        ) t
    group by 1
),

user_settlement as (
    select
        to_varchar(advance_id) as advance_id,
        payment_ds_pst,
        payment_amount
    from payment_details
    union all
    select
        sd.overdraft_id as advance_id,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', sd.created::timestamp) as payment_ds_pst,
        sd.amount as payment_amount
    from settlement_details sd
    inner join settlement_status ss
    on sd.settlement_status_id = ss.id
    where ss.name = 'COMPLETE'

),

users_candidate as (
    select *
    from
    (
        select
            distinct user_id
        from disbursements
        where disbursement_ds_pst >= dateadd('month', -6,  current_date())
            and disbursement_ds_pst < date_trunc('MONTH',  current_date())
        union
        select
            distinct user_id
        from settlements
        where last_payment_date is not null and date(last_payment_date) >= dateadd('month', -6,  date_trunc('MONTH', current_date()))
            and date(last_payment_date) < date_trunc('MONTH', current_date())
    )

),

--- approved_bank_account
approved_bank as (
    select
        advance_requests.user_id,
        count(distinct
            iff(to_numeric(advance_requests.max_approved_amount) is not null,
            advance_requests.bank_account_id, null)) as approved_bank_count
    from fct_advance_approvals as advance_requests
    join users_candidate users
    on advance_requests.user_id = users.user_id
    where advance_requests.requested_ds <= date_trunc('MONTH', current_date())
    group by 1
),

-- Bank related metrics
bank_feature as (
    select
        uc.user_id,
        nur.platform,
        nur.attribution,
        nur.network,
        nur.bod_account_open_user,
        nur.bod_direct_deposit_user,
        date(nur.pv_ts) as pv_date,
        bc.bank_category,
        bc.has_valid_credentials,
        ab.approved_bank_count
    from users_candidate uc
    left join new_user_attribution nur
    on uc.user_id = nur.user_id
    left join ltv_bankconnection bc
    on uc.user_id = bc.user_id
    left join approved_bank ab
    on uc.user_id = ab.user_id
),

uc_trans_first as (
    select
        uc.user_id,
        ds.advance_id,
        ds.disbursement_ds_pst,
        datediff('day', bf.pv_date, date(ds.disbursement_ds_pst)) as datediff,
        case
        when ds.original_payback_date > date_trunc('MONTH', current_date()) then 1
        else 0
        end as not_fully_baked,
        ss.amount_due,
        ds.taken_amount as principal,
        ss.first_payment_date,
        rank() over (partition by uc.user_id order by ds.disbursement_ds_pst desc) as ranknum
    from users_candidate uc
    join disbursements ds
    on uc.user_id = ds.user_id
    left join settlements ss
    on ds.advance_id = ss.advance_id
    left join bank_feature bf
    on uc.user_id = bf.user_id
    where ds.disbursement_ds_pst <= date_trunc('MONTH', current_date())
    order by uc.user_id, ds.disbursement_ds_pst
),

uc_settle as (
    select
        ut.advance_id,
        sum(us.payment_amount) as total_settled_amount
    from uc_trans_first ut
    left join user_settlement us
    on ut.advance_id = us.advance_id
    where us.payment_ds_pst < date_trunc('MONTH', current_date())
    group by 1
),

uc_trans as (
    select
        utf.user_id,
        utf.advance_id,
        utf.disbursement_ds_pst,
        us.total_settled_amount,
        us.total_settled_amount - utf.principal as net_rev,
        utf.datediff,
        utf.not_fully_baked,
        IFF(utf.amount_due <= us.total_settled_amount, 0, utf.amount_due - us.total_settled_amount) as outstanding_amount,
        utf.amount_due,
        utf.principal,
        case
        when utf.first_payment_date < date_trunc('MONTH', dateadd('month', -1, current_date())) then utf.first_payment_date
        else null
        end as first_payment_date_update,
        case
        when first_payment_date_update is not null then datediff('day', date(utf.disbursement_ds_pst), date(first_payment_date_update))
        else null
        end as settlement_dis,
        utf.ranknum
    from uc_trans_first utf
    left join uc_settle us
    on utf.advance_id = us.advance_id
),
rmf as (
    select
        user_id,
        date(min(disbursement_ds_pst)) as first_trans,
        count(advance_id) as frequency,
        datediff('day', date(min(disbursement_ds_pst)), date(max(disbursement_ds_pst))) as recency,
        datediff('day', date(min(disbursement_ds_pst)), date_trunc('MONTH', current_date())) as T,
        avg(
            case
            when not_fully_baked = 0 then net_rev
            else null
            end
        ) as monetary,
        sum(
            case
                when not_fully_baked = 0 then net_rev
                else null
                end
        ) as monetary_total,
        max(not_fully_baked) as not_baked,
        max(net_rev) as max_rev,
        max(principal) as max_principle,
        stddev(settlement_dis) as settlement_date_dev,
        stddev(datediff) as disburse_date_dev,
        stddev(net_rev) as net_rev_dev,
        regr_slope(
                case
                when not_fully_baked = 1 then null
                else total_settled_amount
                end,
                case
                when not_fully_baked = 1 then null
                else datediff
                end
            ) as slope,
            regr_intercept(
                case
                when not_fully_baked = 1 then null
                else total_settled_amount
                end,
                case
                when not_fully_baked = 1 then null
                else datediff
                end
            ) as intercept
    from uc_trans
    group by user_id
),

last_trans as (
    select
        user_id,
        outstanding_amount / (0.01 + amount_due) as settled_rate,
        outstanding_amount
    from uc_trans
    where ranknum = 1

),

prior_revenue as (
    select
        uc.user_id,
        sum(ss.total_settled_amount - ss.principal) as prior_revenue
    from users_candidate uc
    join settlements ss
    on uc.user_id = ss.user_id
    where ss.settlement_due_ds_pst >= dateadd('month', -6, date_trunc('MONTH', current_date()))
        and ss.settlement_due_ds_pst < date_trunc('MONTH', current_date())
    group by 1
),

revenue as (
    select
        uc.user_id,
        sum(ss.total_settled_amount - ss.principal) as sumrevenue,
        sum(greatest(ss.total_settled_amount - ss.principal, 0)) as sumpledge
    from users_candidate uc
    join settlements ss
    on uc.user_id = ss.user_id
    where ss.settlement_due_ds_pst >= date_trunc('MONTH', current_date())
        and ss.settlement_due_ds_pst < dateadd('month', 6, date_trunc('MONTH', current_date()))
    group by 1
),

final as (
    select
        bf.user_id,
        coalesce(bf.platform, 'None') as platform,
        coalesce(bf.attribution, 'None') as attribution,
        coalesce(bf.network, 'None') as network,
        coalesce(bf.bod_account_open_user, 0) as bod_account_open_user,
        coalesce(bf.bod_direct_deposit_user, 0) as bod_direct_deposit_user,
        coalesce(bf.bank_category, 'None') as bank_category,
        coalesce(bf.has_valid_credentials, 0) as has_valid_credentials,
        coalesce(bf.approved_bank_count , 0) as approved_bank_count,
        coalesce(rmf.first_trans, null) as first_trans,
        coalesce(rmf.frequency, 0) as frequency,
        coalesce(rmf.recency, 0) as recency,
        coalesce(rmf.T, 0) as T,
        coalesce(rmf.monetary, 0) as monetary,
        coalesce(rmf.monetary_total, 0) as monetary_total,
        coalesce(rmf.max_rev, 0) as max_rev,
        coalesce(rmf.max_principle, 0) as max_principle,
        coalesce(rmf.not_baked, 0) as not_baked,
        coalesce(rmf.settlement_date_dev, 0) as settlement_date_dev,
        coalesce(rmf.disburse_date_dev, 0) as disburse_date_dev,
        coalesce(rmf.net_rev_dev, 0) as net_rev_dev,
        coalesce(rmf.slope, 0) as slope,
        coalesce(rmf.intercept, 0) as intercept,
        coalesce(lt.settled_rate, 0) as settled_rate,
        coalesce(lt.outstanding_amount, 0) as outstanding_amount,
        coalesce(revenue.sumrevenue, 0.0) as sumrevenue,
        coalesce(revenue.sumpledge, 0.0) as sumpledge,
        coalesce(ui.monthly_income, 0) as monthly_income,
        coalesce(pr.prior_revenue, 0) as prior_revenue,
        date_trunc('MONTH', current_date()) as forecast_date
    from bank_feature bf
    left join rmf
    on bf.user_id = rmf.user_id
    left join last_trans lt
    on bf.user_id = lt.user_id
    left join revenue
    on bf.user_id = revenue.user_id
    left join user_income as ui
    on bf.user_id = ui.user_id
    left join prior_revenue as pr
    on bf.user_id = pr.user_id
)
select
    *,
    case
        when sumrevenue < 0.1 then 0
        else 1
    end as if_retention
from final

