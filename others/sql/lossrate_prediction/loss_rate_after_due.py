# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    # Your code goes here, inside the "main" handler.
    query_string = """
        SELECT
        disbursement_ds,
        SUM(amount_due) AS total_receivables,
        SUM(advance_amount) AS total_disbursement_amount,
        SUM(pledged_revenue) AS total_pledged_revenue,

    """
    query_string2 = """ """

    for n_day in range(0, 362):
        tmp_string = f"""
             1 - DIV0(SUM(settled_{ n_day }_days_after_due), total_receivables) AS D_{ n_day }_loss_rate,

        """
        query_string2 += tmp_string

    query_string3 = """
        total_receivables - total_disbursement_amount - total_pledged_revenue AS diff
        FROM SANDBOX.DEV_YYANG.settlements_original_payback_date_loss_arrival
        GROUP BY 1
        ORDER BY 1 DESC
    """
    query_str = query_string + query_string2 + query_string3

    df = session.sql(query_str)
    df.write.mode("overwrite").save_as_table("sandbox.dev_yyang.lossrate_daily", table_type="")
    return type(df)