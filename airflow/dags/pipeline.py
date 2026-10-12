from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
# import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
import requests


@dag(
    dag_id='uber_pipeline'
)
def uber_pipeline():

    run_databricks_job = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        databricks_conn_id='databricks_conn',
        job_id=851975752952213
    )


uber_pipeline()