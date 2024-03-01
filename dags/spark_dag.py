from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

home_dir = config.get('Common', 'home_dir')

impressions_path = config.get('SparkDag', 'impressions_path')
clicks_path = config.get('SparkDag', 'clicks_path')
bid_requests_path = config.get('SparkDag', 'bid_requests_path')
website_conversion_rate_path = config.get('SparkDag', 'website_conversion_rate_path')
avg_spent_path = config.get('SparkDag', 'avg_spent_path')
user_path = config.get('SparkDag', 'user_path')
funnel_path = config.get('SparkDag', 'funnel_path')
campaign_ctr_path = config.get('SparkDag', 'campaign_ctr_path')
user_performance_path = config.get('SparkDag', 'user_performance_path')
stream_mode = config.get('SparkDag', 'stream_mode')
checkpoint_location = config.get('SparkDag', 'checkpoint_location')

args_list = [
"--impressions_path",f"{home_dir}/{impressions_path}",
"--clicks_path",f"{home_dir}/{clicks_path}",
"--bid_requests_path",f"{home_dir}/{bid_requests_path}",
"--website_conversion_rate_path",f"{home_dir}/{website_conversion_rate_path}",
"--avg_spent_path",f"{home_dir}/{avg_spent_path}",
"--user_path",f"{home_dir}/{user_path}",
"--funnel_path",f"{home_dir}/{funnel_path}",
"--campaign_ctr_path",f"{home_dir}/{campaign_ctr_path}",
"--user_performance_path",f"{home_dir}/{user_performance_path}",
"--stream_mode",stream_mode,
"--checkpoint_location",f"{home_dir}/{checkpoint_location}",
]

with DAG(
    dag_id="my_spark_dag1.0",
    start_date=datetime(2024,1,1),
    schedule_interval='@daily') as dag:

    submit_job = SparkSubmitOperator(
        task_id='run_pyspark_job',
        application=f'{home_dir}/include/addX.py',
        application_args=args_list,
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
