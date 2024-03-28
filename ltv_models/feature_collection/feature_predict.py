import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd
import numpy as np
import math


def main(session: snowpark.Session):
    # Your code goes here, inside the "main" handler.
    #snowflake.connector.paramstyle='numeric'
    def get_trending(row):
        X = np.array([float(x) for x in row['MONTHLIST'].strip().split(',')])
        Y = np.array([float(x) for x in row['REVLIST'].strip().split(',')])
        print(X)
        if len(X) == 0 or len(X) == 1:
            return 0, 0, 1, 1

        trending_ratio = np.sum((X-np.mean(X))*(Y-np.mean(Y)))/(np.sum(np.square(X-np.mean(X))) + 0.001)
        intercept = np.mean(Y) - trending_ratio * np.mean(X)
        disburstdate_std = np.std(X)
        rev_std = np.std(Y)
        return trending_ratio, intercept,disburstdate_std, rev_std

    dateset = [
        '2024-02-01',
    ]


    query_str = """
    With
    -- For calculating the approved_bank_count
    FCT_ADVANCE_APPROVALS AS
    (
        SELECT * FROM ANALYTIC_DB.DBT_marts.fct_advance_approvals
    ),

    FCT_ADVANCES AS
    (
        SELECT * FROM ANALYTIC_DB.DBT_marts.fct_advances
    ),

    FCT_OVERDRAFT_DISBURSEMENT AS
    (
        SELECT * FROM ANALYTIC_DB.DBT_marts.fct_overdraft_disbursement
    ),

    disbursements as (
        select * from ANALYTIC_DB.DBT_marts.disbursements
    ),

    settlements as (
        select * from ANALYTIC_DB.DBT_marts.settlements
    ),

    new_user_attribution as (
        select * from ANALYTIC_DB.DBT_marts.new_user_attribution
    ),

    ltv_bankconnection as (
        select *, rank() over (partition by user_id order by has_valid_credentials desc) as ranknum
        from ANALYTIC_DB.DBT_metrics.ltv_bankconnection qualify ranknum = 1
    ),

    -- Get users who has to have one disbursement or one settlement within half year of date_par
    users_candidate as (
        select * from (
            select
                distinct user_id
            from disbursements
            where disbursement_ds_pst >= dateadd('month', -6,  date(?)) AND disbursement_ds_pst < date(?)
            union
            select
                distinct user_id
            from settlements
            where last_payment_date is not null and date(last_payment_date) >= dateadd('month', -6,  date(?)) AND date(last_payment_date) < date(?)
        ) sample (3000 rows)
    ),
    --select * from users_candidate

    --- approved_bank_account
    APPROVED_BANK AS (
        SELECT
            advance_requests.USER_ID,
            COUNT(DISTINCT
                IFF(TO_NUMERIC(advance_requests.MAX_APPROVED_AMOUNT) IS NOT NULL,
                advance_requests.BANK_ACCOUNT_ID, NULL)) AS APPROVED_BANK_COUNT
        FROM FCT_ADVANCE_APPROVALS AS advance_requests
        JOIN users_candidate USERS
        ON ADVANCE_REQUESTS.USER_ID = USERS.USER_ID
        WHERE ADVANCE_REQUESTS.REQUESTED_DS <= date(?)
        GROUP BY 1
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

    uc_trans as (
        select
            uc.user_id,
            ds.advance_id,
            ds.disbursement_ds_pst,
            ss.total_settled_amount,
            ss.total_settled_amount - ss.principal as net_rev,
            datediff('day', date(ds.disbursement_ds_pst), date(?)) as datediff,
            case
            when ss.settlement_due_ds_pst > date(?) then 1
            else 0
            end as not_fully_baked,
            ss.outstanding_amount,
            ss.amount_due,
            ss.principal,
            ss.first_payment_date,
            case
            when ss.first_payment_date is not null then datediff('day', date(ds.disbursement_ds_pst), date(ss.first_payment_date))
            else null
            end as settlement_dis,
            rank() over (partition by uc.user_id order by ds.disbursement_ds_pst desc) as ranknum
        from users_candidate uc
        join disbursements ds
        on uc.user_id = ds.user_id
        join settlements ss
        on ds.advance_id = ss.advance_id
        where ds.disbursement_ds_pst <= date(?)
        order by uc.user_id, ds.disbursement_ds_pst
    ),
    user_income as (
        SELECT user_id, SUM(LAST_30D_INCOME) as monthly_income
        FROM
        (SELECT *
        FROM DAVE.INCOME_DETECTION_INCOME_DETECTION.PAYROLL_ASSESSMENT
        WHERE
        NUM_VALID_PAYROLL_GROUPS >= 1
        AND _fivetran_deleted = 'false'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id, bank_account_id ORDER BY ASSESSMENT_DATE_TIME DESC) = 1
        ) t
        GROUP BY 1
    ),

    rmf as (
        select
            user_id,
            date(min(disbursement_ds_pst)) as first_trans,
            count(advance_id) as frequency,
            datediff('day', date(min(disbursement_ds_pst)), date(max(disbursement_ds_pst))) as recency,
            datediff('day', date(min(disbursement_ds_pst)), date(?)) as T,
            avg(
                case
                when not_fully_baked = 0 then net_rev
                else null
                end
            ) as monetary,
            sum(net_rev) as monetary_total,
            max(not_fully_baked) as not_baked,
            max(net_rev) as max_rev,
            max(principal) as max_principle,
            stddev(settlement_dis) as settlement_dev,
            array_to_string(array_agg(datediff), ',') as monthlist,
            array_to_string(array_agg(net_rev), ',') as revlist
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
        where ss.settlement_due_ds_pst >= dateadd('month', -6, ?) AND ss.settlement_due_ds_pst < date(?)
        group by 1
    ),

    revenue as (
        select
            uc.user_id,
            sum(ss.total_settled_amount - ss.principal) as sumrevenue
        from users_candidate uc
        join settlements ss
        on uc.user_id = ss.user_id
        where ss.settlement_due_ds_pst >= date(?) AND ss.settlement_due_ds_pst < dateadd('month', 6, ?)
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
            coalesce(rmf.settlement_dev, 0) as settlement_dev,
            rmf.monthlist,
            rmf.revlist,
            coalesce(lt.settled_rate, 0) as settled_rate,
            coalesce(lt.outstanding_amount, 0) as outstanding_amount,
            coalesce(revenue.sumrevenue, 0.0) as sumrevenue,
            coalesce(ui.monthly_income, 0) as monthly_income,
            coalesce(pr.prior_revenue, 0) as prior_revenue,
            date(?) as forecast_date
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
    """
    categorical_features = [
        'PLATFORM',
        'ATTRIBUTION'
    ]
    dflist = []
    for datepar in dateset:
        df = session.sql(query_str,
                         params=[datepar, datepar,datepar,datepar,datepar,datepar,datepar,datepar,datepar,datepar, datepar, datepar, datepar, datepar])
        dflist.append(df)
    dfsnow = dflist[0]
    df_combine = dfsnow.to_pandas()
    rows = df_combine.shape[0]
    df_con = []
    bins = int(rows / 10000)
    for i in range(1, bins):
        if i != bins - 1:
            dfcombine = df_combine.iloc[(i-1) * 10000: i * 10000]
        else:
            dfcombine = df_combine.iloc[(i-1) * 10000: ]
        dfcombine['forecastdate'] = pd.to_datetime(dfcombine['FORECAST_DATE'])
        dfcombine['month'] = dfcombine['forecastdate'].dt.month
        dfcombine['month_sin'] = np.sin(2 * math.pi * dfcombine['month'] / 12)
        dfcombine['month_cos'] = np.cos(2 * math.pi * dfcombine['month'] / 12)
        dfcombine[['trending','intercept', 'disburse_std', 'nev_std']] = dfcombine.apply(lambda row:get_trending(row), axis=1).apply(pd.Series)

        df_data= pd.get_dummies(dfcombine, prefix=['platform', 'attribution'],
                                prefix_sep='_', dummy_na=True,
                                columns=categorical_features,
                                sparse=False, drop_first=False, dtype=None)
        df_data.drop(columns=['FORECAST_DATE', 'BANK_CATEGORY', 'FIRST_TRANS', 'NETWORK'], inplace=True)
        df_con.append(df_data)
        print(i)
    dfupdate = pd.concat(df_con, ignore_index=True)
    df_output = session.create_dataframe(dfupdate)
    df_output.write.mode("overwrite").save_as_table("sandbox.dev_yyang.ltv_test_forecast", table_type="")
    return type(df_output)