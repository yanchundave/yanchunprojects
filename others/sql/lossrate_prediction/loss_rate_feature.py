# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    # Your code goes here, inside the "main" handler.
    query_string = """
        WITH tmp as (

        select user_id, disbursement_ds,taken_amount,
        ith_advance_taken as rownumber,
        case
            when taken_amount <= 100 then '0-100-dollar'
            when taken_amount <= 200 then '100-200-dollar'
            when taken_amount <= 300 then '200-300-dollar'
            when taken_amount <= 400 then '300-400-dollar'
            when taken_amount <= 500 then '400-500-dollar'
            else 'over-500-dollar'
        end as amount_bucket
        from sandbox.dev_yyang.lossrate_disbursements
     ),
     tmp_history as (
     select disbursement_ds, rownumber, count(distinct user_id) as count_history from tmp
     group by 1, 2
     ),
     tmp_dis as (
     select disbursement_ds, amount_bucket, count(distinct user_id) as count_amount from tmp
     group by 1, 2
 )
    """
    q1 = """
        select * from tmp_history
    """
    q2 = """
        select * from tmp_dis
    """
    df = session.sql(query_string + q1)
    df.write.mode("overwrite").save_as_table("sandbox.dev_yyang.loss_rate_feature_history", table_type="")

    df1 = session.sql(query_string + q2)
    df1.write.mode("overwrite").save_as_table("sandbox.dev_yyang.loss_rate_feature_disburse", table_type="")
    return type(df)