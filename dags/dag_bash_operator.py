from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dag_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    resize_image_dag = BashOperator(
        task_id="resize_image_dag",
        bash_command="echo whoami",
    )

    resize_image_dag2 = BashOperator(
        task_id="resize_image_dag2",
        bash_command="echo $HOSTNAME",
    )

    resize_image_dag >> resize_image_dag2