"""
This Dag orchestrates the script of da ltv train dataset
"""

import os
from datetime import datetime
from logging import getLogger

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from common import BUCKET, default_args
from domain.dat.da_ltv.da_ltv_operators import DaLTVModelOp

log = getLogger(__file__)

NAME = "daltvtrain"

with DAG(
    NAME,
    default_args=default_args,
    description="ltv long term model training ",
    start_date=datetime(2024, 4, 1),
    schedule_interval="0 0 2 * *",
    catchup=False,
    tags=["dat", "da_ltv_train"],
) as dags:
    start = DummyOperator(task_id="start")
    run_da_ltv_train_task = DaLTVModelOp(
        task_id="run_da_ltv_train",
        working_dir="da_ltv",
        script=os.path.join(BUCKET, "src/domain/dat/da_ltv/da_ltv_train.py"),
    )
    run_da_ltv_predict_task = DaLTVModelOp(
        task_id="run_da_ltv_predict",
        working_dir="da_ltv",
        script=os.path.join(BUCKET, "src/domain/dat/da_ltv/da_ltv_predict.py"),
    )
    end = DummyOperator(task_id="end")

    (
        start
     >> [run_da_ltv_train_task,
         run_da_ltv_predict_task
        ]
    >> end
    )

    log.info("Ran the dag, no syntax errors")
