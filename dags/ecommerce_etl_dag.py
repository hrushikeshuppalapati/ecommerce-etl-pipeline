from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

with DAG(
    dag_id='ecommerce_batch_etl',
    description='Daily ETL job for e-commerce data',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['ecommerce', 'spark', 'etl'],
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_etl_job",
        conn_id="spark_default", 
        application="/opt/bitnami/spark/jobs/process_ecommerce.py",
        packages="org.postgresql:postgresql:42.5.0",
     )
