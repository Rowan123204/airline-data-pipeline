from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add the src folder to the Python path so Airflow can find our modules
sys.path.insert(0, '/opt/airflow/project/src')

# ============================================================
# DEFAULT ARGUMENTS
# ============================================================
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,                        # Retry once on failure
    'retry_delay': timedelta(minutes=5), # Wait 5 minutes before retrying
}

# ============================================================
# TASK FUNCTIONS
# Each function represents one step in the ETL pipeline.
# We use XCom (ti.xcom_push / ti.xcom_pull) to pass the
# output file path between tasks instead of passing DataFrames
# directly (DataFrames are not serializable between Airflow tasks).
# ============================================================

def run_extract(**context):
    """
    Task 1 - EXTRACT:
    Reads raw CSV files using PySpark and saves them as
    Parquet files in a temporary location (/tmp).
    Pushes the output path to XCom for the next task.
    """
    from extract import extract

    flights, airports, airlines = extract()

    # Save DataFrames to temp Parquet files so Transform task can read them
    flights.write.mode("overwrite").parquet("/tmp/raw_flights")
    airports.write.mode("overwrite").parquet("/tmp/raw_airports")
    airlines.write.mode("overwrite").parquet("/tmp/raw_airlines")

    # Push the paths to XCom so the Transform task can find them
    context['ti'].xcom_push(key='flights_path',  value='/tmp/raw_flights')
    context['ti'].xcom_push(key='airports_path', value='/tmp/raw_airports')
    context['ti'].xcom_push(key='airlines_path', value='/tmp/raw_airlines')

    print("✅ Extract complete. Raw data saved to /tmp.")


def run_transform(**context):
    """
    Task 2 - TRANSFORM:
    Pulls the raw Parquet paths from XCom.
    Runs the PySpark transformation logic.
    Saves the cleaned DataFrame to /tmp as Parquet.
    Pushes the output path to XCom for the Load task.
    """
    from spark import spark
    from transform import transform

    # Pull paths written by the Extract task
    ti = context['ti']
    flights_path  = ti.xcom_pull(task_ids='extract_task', key='flights_path')
    airports_path = ti.xcom_pull(task_ids='extract_task', key='airports_path')
    airlines_path = ti.xcom_pull(task_ids='extract_task', key='airlines_path')

    # Read the raw Parquet files back into DataFrames
    flights  = spark.read.parquet(flights_path)
    airports = spark.read.parquet(airports_path)
    airlines = spark.read.parquet(airlines_path)

    # Run the transformation logic
    final_df = transform(flights, airports, airlines)

    # Save transformed data to a temp location
    final_df.write.mode("overwrite").parquet("/tmp/transformed_flights")

    # Push the output path for the Load task
    ti.xcom_push(key='transformed_path', value='/tmp/transformed_flights')

    print("✅ Transform complete. Cleaned data saved to /tmp.")


def run_load(**context):
    """
    Task 3 - LOAD:
    Pulls the transformed Parquet path from XCom.
    Writes the final cleaned dataset to the Gold layer (data/gold).
    """
    from spark import spark
    from load import load

    # Pull path written by the Transform task
    ti = context['ti']
    transformed_path = ti.xcom_pull(task_ids='transform_task', key='transformed_path')

    # Read the transformed Parquet back into a DataFrame
    final_df = spark.read.parquet(transformed_path)

    # Write to the final Gold layer
    load(final_df)

    print("✅ Load complete. Gold data written to data/gold.")


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    'airline_pyspark_pipeline',
    default_args=default_args,
    description='ELT Pipeline: Extract raw CSVs -> Transform with PySpark -> Load to Gold Parquet',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # ---- TASK 1: EXTRACT ----
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=run_extract,
        provide_context=True,
    )

    # ---- TASK 2: TRANSFORM ----
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=run_transform,
        provide_context=True,
    )

    # ---- TASK 3: LOAD ----
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=run_load,
        provide_context=True,
    )

    # ============================================================
    # TASK DEPENDENCIES (The "Directed" part of DAG)
    # extract_task must finish before transform_task starts.
    # transform_task must finish before load_task starts.
    # ============================================================
    extract_task >> transform_task >> load_task
