from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timedelta

import requests
import logging
import json

def get_mysql_connection():
    hook = MySqlHook(mysql_conn_id='mysql_conn_id')
    return hook.get_conn().cursor()

@task
def etl(schema, table, lat, lon, api_key):

    # https://openweathermap.org/api/one-call-api
    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"
    response = requests.get(url)
    data = json.loads(response.text)

    ret = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append(f"('{day}', {d['temp']['day']}, {d['temp']['min']}, {d['temp']['max']})")

    cur = get_mysql_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date DATE,
    temp FLOAT,
    min_temp FLOAT,
    max_temp FLOAT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    logging.info(create_table_sql)

    # 임시 테이블 생성
    create_temp_table_sql = f"""CREATE TEMPORARY TABLE t AS SELECT * FROM {schema}.{table} WHERE 1=0;"""
    logging.info(create_temp_table_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_temp_table_sql)
        cur.connection.commit()
    except Exception as e:
        cur.connection.rollback()
        raise

    # 임시 테이블 데이터 입력
    insert_temp_table_sql = f"INSERT INTO t (date, temp, min_temp, max_temp) VALUES " + ",".join(ret)
    logging.info(insert_temp_table_sql)
    try:
        cur.execute(insert_temp_table_sql)
        cur.connection.commit()
    except Exception as e:
        cur.connection.rollback()
        raise

    # 기존 테이블 대체
    replace_table_sql = f"""DELETE FROM {schema}.{table};
      INSERT INTO {schema}.{table} (date, temp, min_temp, max_temp)
      SELECT date, temp, min_temp, max_temp FROM t;"""
    logging.info(replace_table_sql)
    try:
        cur.execute(replace_table_sql)
        cur.connection.commit()
    except Exception as e:
        cur.connection.rollback()
        raise

with DAG(
    dag_id='Weather_to_MySQL_v2',
    start_date=datetime(2022, 8, 24),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 4 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("your_schema", "weather_forecast_v2", 37.5665, 126.9780, Variable.get("open_weather_api_key"))
