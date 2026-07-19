from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Basic DAG settings
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1), # Start date
    'retries': 1,                       # Retry once on failure
    'retry_delay': timedelta(minutes=5),# Wait 5 minutes before retrying
}

# Define the DAG
with DAG(
    'airline_pyspark_pipeline',
    default_args=default_args,
    schedule_interval='@daily', # Run once a day
    catchup=False
) as dag:

    # Task 1: Run the main.py pipeline
    run_pipeline = BashOperator(
        task_id='run_pyspark_main',
        # Path inside the Docker container
        bash_command='cd /opt/airflow/project/src && python3 main.py' 
    )

    # Task dependencies
    run_pipeline
