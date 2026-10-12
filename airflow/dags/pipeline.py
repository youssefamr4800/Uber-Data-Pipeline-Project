from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
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

    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/uber_dbt && dbt run --models silver --profiles-dir /opt/airflow/.dbt'
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/uber_dbt && dbt snapshot --profiles-dir /opt/airflow/.dbt'
    )

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/uber_dbt && dbt run --models gold --profiles-dir /opt/airflow/.dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/uber_dbt && dbt test --profiles-dir /opt/airflow/.dbt'
    )

    run_databricks_job >> dbt_run_silver >> dbt_snapshot >> dbt_run_gold >> dbt_test
uber_pipeline()