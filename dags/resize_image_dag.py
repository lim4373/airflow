from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
from PIL import Image

def resize_image(input_path, output_path, size=(224, 224)):
    with Image.open(input_path) as img:
        img = img.resize(size)
        img.save(output_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'resize_image_dag',
    default_args=default_args,
    description='A simple DAG to resize images',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def resize_task(**kwargs):
    input_path = kwargs['input_path']
    output_path = kwargs['output_path']
    resize_image(input_path, output_path)

resize_image_task = PythonOperator(
    task_id='resize_image',
    python_callable=resize_task,
    op_kwargs={
        'input_path': '/path/to/your/input/image.jpg',
        'output_path': '/path/to/your/output/image_resized.jpg',
    },
    dag=dag,
)

resize_image_task
