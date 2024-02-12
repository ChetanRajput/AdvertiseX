from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

home_dir = "/home/crajpur/airflow_spark_project/"

with DAG(
    dag_id="my_spark_dag1.0",
    start_date=datetime(2024,1,1),
    schedule_interval='@daily') as dag:

    submit_job = SparkSubmitOperator(
        task_id='run_pyspark_job',
        application=f'{home_dir}/include/sample_spark.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=False,
        conf={"spark.master": "local"}
    )
    submit_job
