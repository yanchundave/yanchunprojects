with referee as (
select 
  re.*,
  date_trunc('month', date(referee_pv_ts)) as pv_month,
  ad.credit,
  ad.credit_bucket as credit_limit
from sandbox.dev_yyang.referral_analysis_referee re 
left join sandbox.dev_yyang.referral_analysis_advocate ad 
on re.referee_user_id = ad.referee_user_id 

),
referral as (
    select 
    re.*,
    coalesce(re.credit_limit, 'non-referral') as credit_bucket,
    IFF(referee_ftca_ts is not null, referee_user_id, Null) as ftca_user_id,
    IFF(advance_number=2, referee_user_id, Null) as second_advance_user,
    case 
    when requested_ds <= dateadd('day', 30, date(referee_pv_ts)) then referee_user_id
    else NULL
    end as ftca_within30d_user_id,
    total_settled_amount - amount_due as net_revenue
    from referee re 
)
select * from referral