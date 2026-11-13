import json
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable


def calculate():
    ctx = get_current_context()
    parameters = dict(dag_id=ctx['dag'].dag_id, run_id=ctx['run_id'], task_id=ctx['task'].task_id,
                      start=ctx['data_interval_start'].isoformat(), end=ctx['data_interval_end'].isoformat(),
                      dataset='demo')
    subprocess.run([sys.executable, '/opt/airflow/jobs/run.py', '--context', json.dumps(parameters)], check=True)


with DAG('hw23_user_campaigns', start_date=pendulum.datetime(2026, 1, 23, tz='UTC'),
         end_date=pendulum.datetime(2026, 1, 25, tz='UTC'),
         schedule=CronDataIntervalTimetable('0 0 * * *', timezone='UTC'),
         catchup=True, max_active_runs=1, is_paused_upon_creation=True,
         default_args={'retries': 1, 'retry_delay': timedelta(seconds=5)},
         tags=['hw23']) as dag:
    scd2 = PythonOperator(task_id='build_user_scd2', python_callable=calculate)
    recipients = PythonOperator(task_id='build_campaign_recipients', python_callable=calculate)
    scd2 >> recipients
