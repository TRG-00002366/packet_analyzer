from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta


def run_batch_rdd_etl():
    import subprocess
    subprocess.run(["python", "spark/batch_rdd_etl.py"], check=True)

def run_batch_df_etl():
    import subprocess
    subprocess.run(["python", "spark/batch_df_etl.py"], check=True)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "packet_pipeline",
    default_args=default_args,
    schedule_interval="* * * * *",  # every minute
    catchup=False,
) as dag:

    batch_rdd_etl = PythonOperator(
        task_id="batch_rdd_etl",
        python_callable=run_batch_rdd_etl
    )

    batch_df_etl = PythonOperator(
        task_id="batch_df_etl",
        python_callable=run_batch_df_etl
    )

    [batch_rdd_etl, batch_df_etl]

