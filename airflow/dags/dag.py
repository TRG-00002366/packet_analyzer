import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.docker.operators.docker import DockerOperator


SPARK_SUBMIT = "/home/airflow/.local/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
SPARK_SNOWFLAKE_CONNECTOR_PACKAGE = os.getenv(
    "SPARK_SNOWFLAKE_CONNECTOR_PACKAGE",
    "net.snowflake:spark-snowflake_2.12:3.1.1",
)
SNOWFLAKE_JDBC_PACKAGE = os.getenv(
    "SNOWFLAKE_JDBC_PACKAGE",
    "net.snowflake:snowflake-jdbc:3.15.1",
)
SPARK_SNOWFLAKE_PACKAGES = ",".join([
    SPARK_SNOWFLAKE_CONNECTOR_PACKAGE,
    SNOWFLAKE_JDBC_PACKAGE,
])
KAFKA_BOOTSTRAP = "kafka:9092"
PACKET_TOPIC = "packets"
RAW_PATH = "/app/data/raw"
RAW_METADATA_PATH = f"{RAW_PATH}/_spark_metadata"
RAW_CHECKPOINT_PATH = f"{RAW_PATH}/_checkpoints"
RDD_OUTPUT_PATH = "/app/data/transformed/rdd_packets_per_dst_ip"
DF_OUTPUT_PATH = "/app/data/transformed/protocol_distribution"
DBT_PROJECT_DIR = "/app/dbt"
DBT_PROFILES_DIR = "/app/dbt"
DOCKER_NETWORK_NAME = os.getenv("DOCKER_NETWORK_NAME", "spark-net")

SPARK_ENV_ARGS = [
    "--conf", "spark.pyspark.python=python3",
    "--conf", "spark.pyspark.driver.python=python3",
    "--conf", "spark.executorEnv.PYTHONPATH=/app:/app/utils",
    "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
]

logger = logging.getLogger(__name__)


def run_spark_job(script_path, script_args=None, packages=None):
    env = os.environ.copy()
    env["PYTHONPATH"] = "/app:/app/utils"
    env["PYTHONUNBUFFERED"] = "1"

    command = [SPARK_SUBMIT, "--master", SPARK_MASTER, "--verbose", *SPARK_ENV_ARGS]
    if packages:
        command.extend(["--packages", packages])
    command.append(script_path)
    if script_args:
        command.extend(script_args)

    logger.info("Running spark-submit command: %s", " ".join(command))

    process = subprocess.Popen(
        command,
        cwd="/app",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    output_lines = []
    for line in process.stdout:
        stripped_line = line.rstrip()
        output_lines.append(stripped_line)
        logger.info(stripped_line)

    return_code = process.wait()
    if return_code != 0:
        log_tail = "\n".join(output_lines[-40:])
        raise RuntimeError(
            f"spark-submit failed for {script_path} with exit code {return_code}. "
            f"Last Spark output:\n{log_tail}"
        )


def check_kafka_topic():
    from kafka import KafkaConsumer, TopicPartition
    from kafka.admin import KafkaAdminClient

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="airflow-topic-check",
    )
    consumer = None

    try:
        topics = admin_client.list_topics()
        if PACKET_TOPIC not in topics:
            raise RuntimeError(f"Kafka topic '{PACKET_TOPIC}' does not exist")

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            auto_offset_reset="earliest",
        )
        partitions = consumer.partitions_for_topic(PACKET_TOPIC)
        if not partitions:
            raise RuntimeError(f"Kafka topic '{PACKET_TOPIC}' has no partitions")

        topic_partitions = [TopicPartition(PACKET_TOPIC, partition) for partition in partitions]
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)
        total_messages = sum(
            end_offsets[partition] - beginning_offsets[partition]
            for partition in topic_partitions
        )
        if total_messages <= 0:
            raise RuntimeError(f"Kafka topic '{PACKET_TOPIC}' exists but has no messages")

        logger.info(
            "Kafka topic '%s' is available with %s messages across %s partitions",
            PACKET_TOPIC,
            total_messages,
            len(topic_partitions),
        )
    finally:
        if consumer is not None:
            consumer.close()
        admin_client.close()


def run_streaming_job():
    run_spark_job(
        "/app/spark/stream_consumer.py",
        script_args=[
            "--bootstrap-servers", KAFKA_BOOTSTRAP,
            "--topic", PACKET_TOPIC,
            "--output-path", RAW_PATH,
            "--checkpoint-path", RAW_CHECKPOINT_PATH,
        ],
        packages=SPARK_KAFKA_PACKAGE,
    )


def run_rdd_etl():
    run_spark_job(
        "/app/spark/batch_rdd_etl.py",
        packages=SPARK_SNOWFLAKE_PACKAGES,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 15),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


with DAG(
    dag_id="packet_pipeline",
    default_args=default_args,
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    params={
        "execution_date": Param(
            default="2026-03-18",
            type="string",
            description="Logical execution date for this packet pipeline run.",
        )
    },
) as dag:
    start = EmptyOperator(task_id="start")

    check_kafka_topic_task = PythonOperator(
        task_id="check_kafka_topic",
        python_callable=check_kafka_topic,
    )

    run_streaming_job_task = PythonOperator(
        task_id="run_streaming_job",
        python_callable=run_streaming_job,
    )

    wait_for_raw_data = FileSensor(
        task_id="wait_for_raw_data",
        filepath=RAW_METADATA_PATH,
        poke_interval=30,
        timeout=300,
    )

    run_rdd_etl_task = PythonOperator(
        task_id="run_rdd_etl",
        python_callable=run_rdd_etl,
    )

    run_dbt_task = DockerOperator(
        task_id="run_dbt_models",
        image="my-dbt:latest",
        command=f"run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK_NAME,
        environment={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
            "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "PACKET"),
            "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "SILVER"),
            "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DOCKER_NETWORK_NAME": DOCKER_NETWORK_NAME,
        },
        mount_tmp_dir=False,
        auto_remove=True,
    )

    end = EmptyOperator(task_id="end")

    start >> check_kafka_topic_task >> run_streaming_job_task >> wait_for_raw_data >> run_rdd_etl_task >> run_dbt_task >> end