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
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}, {str(location['postcode'])}"
    data['postcode'] = str(location['postcode'])
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']
    return data

def stream_data():
    import json, time
    from kafka import KafkaProducer

    result = get_data()
    result = format_data(result)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=50000)
    producer.send('user_data', json.dumps(result).encode('utf-8'))

    
# with DAG('user_automation',
#         default_args=default_args,
#         schedule_interval='@daily',
#         catchup=False) as dag:

#         streaming_task = PythonOperator(
#             task_id='streaming_task',
#             python_callable=stream_data,
#         )

stream_data()