with tmp90 as 
(
select 
    user_id, 
    count(distinct advance_id) as advancecount90 
from ANALYTIC_DB.DBT_MARTS.disbursements 
where 
    disbursement_ds_pst between  CURRENT_DATE() - 90 AND CURRENT_DATE() 
    and user_id in (17002877, 2430956, 5435789, 9359241, 8478830)
    group by 1
),
tmplast as 
(
select 
    user_id, 
    requested_ds_pst,
    max_approved_amount, 
    is_disbursed, advance_id,
    rank() over (partition by user_id order by requested_ds_pst desc) as rownumber
from analytic_db.dbt_marts.requests
where user_id in (17002877,2430956,5435789,9359241,8478830)
and is_approved = 1 
qualify rownumber = 1
)
select 
    user.id, 
    user.last_name, 
    user.first_name, 
    tmplast.advance_id, 
    tmplast.requested_ds_pst as last_request_date,
    tmplast.max_approved_amount as last_approved_amount, 
    case 
        when is_disbursed = 1 then 'disbursed'
        else 'not_disbursed'
    end as if_disbursed,
    disburse.disbursement_method,
    tmp90.advancecount90
from tmp90 
full join tmplast
    on tmplast.user_id = tmp90.user_id 
left join ANALYTIC_DB.DBT_MARTS.disbursements disburse 
    on tmplast.advance_id = disburse.advance_id
left join APPLICATION_DB.GOOGLE_CLOUD_MYSQL_DAVE.USER user
on tmplast.user_id = user.id
    order by last_name