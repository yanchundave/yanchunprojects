with _referee as
(select user_id as referee_user_id,
        pv_ts as referee_pv_ts,
        campaign as referee_campaign,
        first_advance_taken_amount as referee_ftca_amt,
        first_advance_timestamp as referee_ftca_ts
 from   ANALYTIC_DB.DBT_MARTS.NEW_USER_ATTRIBUTION
 where  True
   and  date(pv_ts) between '2022-01-01' and '2023-01-31'
   and  attribution = 'REFERRAL'
 ),
 
_rfee_ec_approval_raw as 
(select r.referee_user_id,
        advance_approval_id,
        requested_ts,
        requested_ds_pst,
        max_approved_amount
 from   ANALYTIC_DB.DBT_marts.fct_advance_approvals as a 
 inner join _referee as r
         on a.user_id = r.referee_user_id
 ),

_rfee_ec_approval_agg as 
(select referee_user_id,
        count(*) as requested_cnts,
        sum(case when max_approved_amount is not null then 1 else 0 end) as approved_cnts,
        max(max_approved_amount) as max_approved_amount,
        min(requested_ts) as first_requested_ts,
        min(requested_ds_pst) as first_requested_ds_pst
 from   _rfee_ec_approval_raw
 group by 1
 ),

_ec_approval_agg_enriched as 
(select *,
        case when requested_cnts >= 1 then 1 else 0 end ec_requested_flag,
        case when approved_cnts >= 1 then 1 else 0 end ec_approved_flag
 from   (select *
         from   _rfee_ec_approval_agg
         )
 ),

_referrer_raw as
(select m.referrer_id,
        rfee.referee_user_id,
        rfee.referee_pv_ts
 from   _referee as rfee
 inner join "MARKETING_DB"."GOOGLE_CLOUD_MYSQL_PROMOTIONS"."SEGMENT_USER" as m
         on rfee.referee_user_id = m.user_id
 where  rfee.referee_campaign = m.segment_id
 ),
 
_referrer as
(select rfer.*,
        a.pv_ts as referrer_pv_ts,
        a.attribution as referrer_attribution,
        a.first_advance_taken_amount as referrer_ftca_amt,
        a.first_advance_timestamp as referrer_ftca_ts
 from   _referrer_raw as rfer
 left join ANALYTIC_DB.DBT_MARTS.NEW_USER_ATTRIBUTION as a
        on rfer.referrer_id = a.user_id
 ),

_referrer_dd_raw as
(select rfer.referrer_id,
        rfer.referee_user_id,
        t.transaction_amount,
        date(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', t.transaction_ds)) as transaction_ds,
        date(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', t.transaction_ds)) - date(rfer.referee_pv_ts) as days_to_tran
 from   ANALYTIC_DB.DBT_MARTS.DIRECT_DEPOSIT_USERS as t
 inner join _referrer as rfer
         on rfer.referrer_id = t.user_id
 where  True
   and  date(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', t.transaction_ds)) - date(rfer.referee_pv_ts) <= 0
 ),

_referrer_dd as
(select referrer_id,
        referee_user_id,
        sum(case when days_to_tran >= -28 then 1 else 0 end) as rfer_d28_dd_cnts,
        sum(case when days_to_tran >= -28 then transaction_amount else 0 end) as rfer_d28_dd_amt,
        case when sum(case when days_to_tran >= -28 then 1 else 0 end) >= 1 then 1 else 0 end as rfer_d28_dd_flag,
        count(*) as rfer_total_dd_cnts,
        sum(transaction_amount) as rfer_total_dd_amt,
        1 as rfer_total_dd_flag
 from   _referrer_dd_raw
 group by 1,2
 ),

_referrer_debit_raw as
(select rfer.referrer_id,
        rfer.referee_user_id,
        t.transaction_amount,
        t.transaction_ts_pst,
        date(t.transaction_ts_pst) - date(rfer.referee_pv_ts) as days_to_tran
 from   ANALYTIC_DB.DBT_marts.fct_transactions as t
 inner join _referrer as rfer
         on rfer.referrer_id = t.user_id
 where  True
   and  t.is_spend_txn = 1
   and  date(t.transaction_ts_pst) - date(rfer.referee_pv_ts) <= 0
 ),

_referrer_debit as 
(select referrer_id,
        referee_user_id,
        sum(case when days_to_tran >= -28 then 1 else 0 end) as rfer_d28_debit_cnts,
        sum(case when days_to_tran >= -28 then transaction_amount else 0 end) as rfer_d28_debit_amt,
        case when sum(case when days_to_tran >= -28 then 1 else 0 end) >= 1 then 1 else 0 end as rfer_d28_debit_flag,
        count(*) as rfer_total_debit_cnts,
        sum(transaction_amount) as rfer_total_debit_amt,
        1 as rfer_total_debit_flag
 from   _referrer_debit_raw
 group by 1,2
 ),

_referrer_acct_funding_raw as
(select rfer.referrer_id,
        rfer.referee_user_id,
        t.transaction_amount,
        t.transaction_ts_pst,
        date(t.transaction_ts_pst) - date(rfer.referee_pv_ts) as days_to_tran
 from   ANALYTIC_DB.DBT_marts.fct_transactions as t
 inner join _referrer as rfer
         on rfer.referrer_id = t.user_id
 where  True
   and  t.is_funding_txn = 1
   and  date(t.transaction_ts_pst) - date(rfer.referee_pv_ts) <= 0
 ),

_referrer_acct_funding as 
(select referrer_id,
        referee_user_id,
        sum(case when days_to_tran >= -28 then 1 else 0 end) as rfer_d28_fund_cnts,
        sum(case when days_to_tran >= -28 then transaction_amount else 0 end) as rfer_d28_fund_amt,
        case when sum(case when days_to_tran >= -28 then 1 else 0 end) >= 1 then 1 else 0 end as rfer_d28_fund_flag,
        count(*) as rfer_total_fund_cnts,
        sum(transaction_amount) as rfer_total_fund_amt,
        1 as rfer_total_fund_flag
 from   _referrer_acct_funding_raw
 group by 1,2
 ),

_referrer_ecadv_raw as
(select referrer_id,
        referee_user_id,
        'adv' as src,
        t.disbursed_amount,
        t.created_date,
        date(t.created_date) - date(rfer.referee_pv_ts) as days_to_tran
 from   ANALYTIC_DB.DBT_marts.fct_Advances_Collection as t
 inner join _referrer as rfer
         on rfer.referrer_id = t.user_id
 where  True
   and  date(t.created_date) - date(rfer.referee_pv_ts) <= 0
   
 union all
 
 select referrer_id,
        referee_user_id,
        'ec' as src,
        t.disbursed_amount,
        t.created_date,
        date(t.created_date) - date(rfer.referee_pv_ts) as days_to_tran
 from   ANALYTIC_DB.DBT_marts.fct_ExtraCash_Collection as t
 inner join _referrer as rfer
         on rfer.referrer_id = t.user_id
 where  True
   and  date(t.created_date) - date(rfer.referee_pv_ts) <= 0
 ),

_referrer_ecadv as 
(select referrer_id,
        referee_user_id,
        sum(case when days_to_tran >= -28 then 1 else 0 end) as rfer_d28_ecadv_cnts,
        sum(case when days_to_tran >= -28 then disbursed_amount else 0 end) as rfer_d28_ecadv_amt,
        case when sum(case when days_to_tran >= -28 then 1 else 0 end) >= 1 then 1 else 0 end as rfer_d28_ecadv_flag,
        count(*) as rfer_total_ecadv_cnts,
        sum(disbursed_amount) as rfer_total_ecadv_amt,
        1 as rfer_total_ecadv_flag
 from   _referrer_ecadv_raw
 group by 1,2
 ),

_referrer_enriched as
(select rfer.*,

        coalesce(dd.rfer_d28_dd_cnts, 0) as rfer_d28_dd_cnts,
        coalesce(dd.rfer_d28_dd_amt, 0) as rfer_d28_dd_amt,
        coalesce(dd.rfer_d28_dd_flag, 0) as rfer_d28_dd_flag,
        coalesce(dd.rfer_total_dd_cnts, 0) as rfer_total_dd_cnts,
        coalesce(dd.rfer_total_dd_amt, 0) as rfer_total_dd_amt,
        coalesce(dd.rfer_total_dd_flag, 0) as rfer_total_dd_flag,

        coalesce(debit.rfer_d28_debit_cnts, 0) as rfer_d28_debit_cnts,
        coalesce(debit.rfer_d28_debit_amt, 0) as rfer_d28_debit_amt,
        coalesce(debit.rfer_d28_debit_flag, 0) as rfer_d28_debit_flag,
        coalesce(debit.rfer_total_debit_cnts, 0) as rfer_total_debit_cnts,
        coalesce(debit.rfer_total_debit_amt, 0) as rfer_total_debit_amt,
        coalesce(debit.rfer_total_debit_flag, 0) as rfer_total_debit_flag,
        
        -- coalesce(fund.rfer_d28_fund_cnts, 0) as rfer_d28_fund_cnts,
        -- coalesce(fund.rfer_d28_fund_amt, 0) as rfer_d28_fund_amt,
        -- coalesce(fund.rfer_d28_fund_flag, 0) as rfer_d28_fund_flag,
        
        coalesce(ecad.rfer_d28_ecadv_cnts, 0) as rfer_d28_ecadv_cnts,
        coalesce(ecad.rfer_d28_ecadv_amt, 0) as rfer_d28_ecadv_amt,
        coalesce(ecad.rfer_d28_ecadv_flag, 0) as rfer_d28_ecadv_flag,
        coalesce(ecad.rfer_total_ecadv_cnts, 0) as rfer_total_ecadv_cnts,
        coalesce(ecad.rfer_total_ecadv_amt, 0) as rfer_total_ecadv_amt,
        coalesce(ecad.rfer_total_ecadv_flag, 0) as rfer_total_ecadv_flag
  
 from   _referrer as rfer
 left join _referrer_dd as dd
        on rfer.referrer_id = dd.referrer_id
       and rfer.referee_user_id = dd.referee_user_id
 left join _referrer_debit as debit
        on rfer.referrer_id = debit.referrer_id
       and rfer.referee_user_id = debit.referee_user_id
 left join _referrer_acct_funding as fund
        on rfer.referrer_id = fund.referrer_id
       and rfer.referee_user_id = fund.referee_user_id
 left join _referrer_ecadv as ecad
        on rfer.referrer_id = ecad.referrer_id
       and rfer.referee_user_id = ecad.referee_user_id
 ),

_final_raw as 
(select rfee.*,
        rfer.* exclude (referee_user_id, referee_pv_ts),
        coalesce(apvl.ec_requested_flag, 0) as ec_requested_flag ,
        coalesce(apvl.ec_approved_flag, 0) as ec_approved_flag,
        apvl.max_approved_amount as max_approved_amount,
        1 as size
 from   _referee as rfee
 left join _referrer_enriched as rfer
        on rfee.referee_user_id = rfer.referee_user_id
 left join _ec_approval_agg_enriched as apvl
        on rfee.referee_user_id = apvl.referee_user_id 
 ),

_final as (select * from _final_raw)

select *
from   _final