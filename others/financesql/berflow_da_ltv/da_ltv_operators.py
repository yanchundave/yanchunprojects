from airflow.models import Variable

from common import SNOWFLAKE_PASSWORD, SNOWFLAKE_USER
from operators import PythonPodOperator

ENV_VARS = {
    "SNOWFLAKE_ACCOUNT": Variable.get("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_ROLE": Variable.get("SNOWFLAKE_ROLE"),
    "SNOWFLAKE_WAREHOUSE": Variable.get("SNOWFLAKE_WAREHOUSE"),
}


class DaLTVModelOp(PythonPodOperator):
    """
    This is an operator that runs LTV models
    """

    def __init__(self, **kwargs):
        super().__init__(
            requirements="src/domain/dat/ltv_models/requirements.txt",
            env_vars=ENV_VARS,
            secrets=[SNOWFLAKE_USER, SNOWFLAKE_PASSWORD],
            **kwargs,
        )