from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow_dbt.operators.dbt_operator import (
    DbtDepsOperator
)
from typing import Final

dir: Final[str] = '/opt/airflow/dbt'
dbt_bin: Final[str] = '/home/airflow/.local/bin/dbt'

default_args = {
  'dir': f'{dir}',
  'start_date': days_ago(1),
  'dbt_bin': f'{dbt_bin}'
}

with DAG(dag_id='run_dbt_init_tasks', default_args=default_args, schedule_interval='@once', ) as dag:

  dbt_deps = DbtDepsOperator(
    task_id='dbt_deps',
  )

  dbt_compile = BashOperator(
        task_id='dbt_compile',
        bash_command=f"cd {dir}; {dbt_bin} compile",
        dag=dag,
    )

  generate_dbt_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command='dbt docs generate --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    dag=dag,
  )
  

dbt_deps >> dbt_compile >> generate_dbt_docs