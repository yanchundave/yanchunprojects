-- 9 days after funding date

with availabledata as
(
    select *
    from DBT.DEV_YANCHUN_PUBLIC.multi_draw_right_after
    where rightafter <= 9
)
select
     count(*),
     sum(COMPETITOR_AMOUNT) as total_competitor_amt,
     sum(R_AMOUNT) as dave_request_amt,
     sum(t_amount) as dave_amt
from availabledata
