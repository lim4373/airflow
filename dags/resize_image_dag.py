from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import cv2
import os

def resize_image(input_path, output_path, size=(224, 224)):
    img = cv2.imread(input_path)
    resized_img = cv2.resize(img, size)
    cv2.imwrite(output_path, resized_img)

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
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    file_name = os.path.basename(input_path)
    output_file_path = os.path.join(output_path, f"resized_{file_name}")
    resize_image(input_path, output_file_path)

resize_image_task = PythonOperator(
    task_id='resize_image',
    python_callable=resize_task,
    op_kwargs={
        'input_path': r'C:/Users/lim78/airflow/image/성숙/crop_D0_0d4f0dab-60a5-11ec-8402-0a7404972c70.jpg',
        'output_path': r'C:/Users/lim78/airflow/output',
    },
    dag=dag,
)

resize_image_task
