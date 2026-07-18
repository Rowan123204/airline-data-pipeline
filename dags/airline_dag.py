from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# إعدادات الـ DAG الأساسية
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1), # تاريخ بداية التشغيل
    'retries': 1,                       # لو فشل يجرب مرة كمان
    'retry_delay': timedelta(minutes=5),# يستنى 5 دقايق قبل ما يجرب تاني
}

# تعريف الـ DAG
with DAG(
    'airline_pyspark_pipeline',
    default_args=default_args,
    schedule_interval='@daily', # يشتغل مرة كل يوم
    catchup=False
) as dag:

    # Task 1: تشغيل المايسترو (main.py) بالكامل
    run_pipeline = BashOperator(
        task_id='run_pyspark_main',
        # مسار الكود جوه الدوكر اللي هنعمله بعدين
        bash_command='cd /opt/airflow/project && python3 main.py' 
    )

    # ترتيب الـ Tasks
    run_pipeline
