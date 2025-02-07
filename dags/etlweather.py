from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# uri = "mongodb+srv://taufik4051:Dgfandsand#4$35@cluster0.fuyu1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# # Create a new client and connect to the server
# client = MongoClient(uri, server_api=ServerApi('1'))

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
MONGO_CONNECTION_ID = 'mongo_default'
API_CONNECTION_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

#DAG
with DAG(dag_id='weather_etl_pipeline', 
         default_args=default_args, 
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_weather_data():
        """Extracting Weather Data from Open-Meteo API using Airflow Connection"""
        
        # HttpHook to get connection details from AIRFLOW Connection
        http_hook = HttpHook(http_conn_id=API_CONNECTION_ID, method='GET')
        
        # Build the API endpoint
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        # Make the HTTP request via the HTTP hook
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Failed to fetch data from API. Status Code: {response.status_code}')
        
    @task()
    def transform_weather_data(weather_data: dict):
        """Transforming Weather Data"""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'time': weather_data['time'],
            'temperature': current_weather['temperature'],
            'winddirection': current_weather['winddirection'],
            'wind_speed': current_weather['wind_speed'],
            'weather_code': current_weather['weather_code'],
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data: dict):
        """Loading Weather Data to MongoDB"""
        hook = MongoHook(mongo_conn_id = MONGO_CONNECTION_ID)
        client = hook.get_conn()
        db = client.weather
        collection = db["current_weather"]
        collection.insert_one(transformed_data)

    
    ## DAG workflow
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
