from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime.now(),
}

def get_data():
    import json
    import requests 

    response = requests.get('https://randomuser.me/api/')
    response = response.json()
    response = response['results'][0]
    return response

def format_data(response):
    data = {}

def stream_data():
    import json
    
# with DAG('user_automation',
#         default_args=default_args,
#         schedule_interval='@daily',
#         catchup=False) as dag:

#         streaming_task = PythonOperator(
#             task_id='streaming_task',
#             python_callable=stream_data,
#         )

stream_data()