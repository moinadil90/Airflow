from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery_operator import BigQueryOperator
from airflow.providers.redis.operators.redis_publish import RedisPublishOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 5),  # Adjust as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('bq_to_redis_dag', default_args=default_args, schedule_interval='@daily') as dag:

    # Extract data from BigQuery (consider filtering/aggregation for RecSys)
    bq_extract_task = BigQueryOperator(
        task_id='extract_from_bq',
        sql='''
        SELECT *  -- Replace with your actual BigQuery query (consider RecSys)
        FROM `your_project.your_dataset.your_table`
        ''',
        destination_table='gs://your-bucket/bq_data.json',  # Replace with your Cloud Storage bucket
    )

    # Data transformation (optional, consider RecSys logic here)
    def transform_data(data):
        # Implement data transformation logic for RecSys if needed
        # This function might filter, aggregate, or prepare data for Golang services
        return transformed_data

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,  # Pass BQ data to transformation function
        op_args=[bq_extract_task.output],  # Access output of BQ extract task
    )

    # Load data to Redis in JSON format (suitable for Golang)
    redis_load_task = RedisPublishOperator(
        task_id='load_to_redis',
        channel='recsys_data',  # Replace with your desired Redis channel
        key=None,  # No key needed for publish
        value=lambda context: transform_data_task.op_args[0],  # Access transformed data
        redis_conn_id='redis_default',  # Replace with your Redis connection ID
    )

    # Set task dependencies
    bq_extract_task >> transform_data_task >> redis_load_task
