import os
import pandas as pd
import davesci as ds
from da_ltv_udf import get_transformation, log

SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE")

con = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)
con_write = ds.snowflake_connect(warehouse=SNOWFLAKE_WAREHOUSE, role=SNOWFLAKE_ROLE)


def read_train_data():
    sql_str = """
    select *
    from analytic_db.dbt_metrics.da_ltv_dataset
    """
    df = pd.read_sql_query(sql_str, con)
    return df


def main():
    dftrain = read_train_data()
    df = get_transformation(dftrain)

    ds.write_snowflake_table(
        df,
        "davesci.ltv.ltv_train",
        con_write,
        mode="create",
    )

    log.info(df.shape)


if __name__ == "__main__":
    main()
