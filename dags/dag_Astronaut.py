import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

import pathlib # 경로를 문자열이 아닌 객체로 처리하도록 해줌
import json
import requests
import requests.exceptions as requests_exceptions

Astronaut_URL = "https://ll.thespacedevs.com/2.2.0/spacestation/?format=api"
JSON_PATH = '/tmp/astronaut.json'
TARGET_DIR = '/tmp/images'

def _get_pictures():
    # Path() : Path 객체 생성
    # mkdir() - exist_ok=True : 폴더가 없을 경우 자동으로 생성 
    pathlib.Path(TARGET_DIR).mkdir(parents=True, exist_ok=True)

    # launches.json 파일에 있는 모든 그림 파일 download
    with open(JSON_PATH) as f:
        try:
            launches = json.load(f)
            image_urls = [launch['image'] for launch in launches['results']]

            for i, image_url in enumerate(image_urls):
                try:
                    response = requests.get(image_url)
                    image_filename = image_url.split('/')[-1]
                    target_file = f'{TARGET_DIR}/{image_filename}'

                    with open(target_file, 'wb') as f:
                        f.write(response.content)
                    
                    print(f'Downloaded {image_url} to {target_file}')
                except requests_exceptions.MissingSchema: 
                    print(f'{image_url} appears to be an invalid URL.')
                except requests_exceptions.ConnectionError:
                    print(f'Could not connect to {image_url}.')
        except KeyError as e:
            with open(JSON_PATH) as f:
                print(json.load(f)) # ex : {'detail': 'Request was throttled. Expected available in 766 seconds.'}
            raise


with DAG(
    'download_Astronaut',
    start_date = airflow.utils.dates.days_ago(14),
    schedule_interval = None
) as dag:
    
    download_launches = BashOperator(
        task_id = 'download_launches',
        bash_command = f'curl -o {JSON_PATH} -L {Astronaut_URL}'
    )

    get_pictures = PythonOperator(
        task_id = 'get_pictures',
        python_callable = _get_pictures
    )

    notify = EmailOperator(
        task_id = 'send_email',
        to = 'seonglimc36@gmail.com',
        subject = 'Rocket Launches Data Ingestion Completed.',
        html_content = """
            <h2>Rocket Launches Data Ingestion Completed.</h2>
            <br/>
            Date : {{ ds }}
        """
    )

    download_launches >> get_pictures >> notify