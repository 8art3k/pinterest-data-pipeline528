import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime

# Seed for random number generation
random.seed(100)

KAFKA_TOPICS = {
    "pin_data": "262542bdae36.pin",  
    "geo_data": "262542bdae36.geo",  
    "user_data": "262542bdae36.user"  
}

class AWSDBConnector:

    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.db_creds = self.read_db_creds()

    def read_db_creds(self):
        with open(self.yaml_file, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def create_db_connector(self):
        HOST = self.db_creds.get('RDS_HOST')
        USER = self.db_creds.get('RDS_USER')
        PASSWORD = self.db_creds.get('RDS_PASSWORD')
        DATABASE = self.db_creds.get('RDS_DATABASE')
        PORT = self.db_creds.get('RDS_PORT')

        engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector("db_creds.yaml")
engine = new_connector.create_db_connector()

# Function to send data to Kafka
def send_to_kafka(topic, data):
    """Send data to the Kafka topic via the API"""
    
    API_INVOKE_URL = f"https://2s1rfz0va3.execute-api.us-east-1.amazonaws.com/dev/topics/{topic}"
    

    payload = {
        "records": [
            {
                "value": data 
            }
        ]
    }
    
    headers = {
        "Content-Type": "application/vnd.kafka.json.v2+json",
    }

    try:
        response = requests.post(API_INVOKE_URL, json=payload, headers=headers)
        if response.status_code == 200:
            print(f"Data sent to {topic} successfully")
        else:
            print(f"Failed to send data to {topic}, Status Code: {response.status_code}, Response: {response.json()}")
    except Exception as e:
        print(f"Error sending data to {topic}: {e}")

def convert_datetime_to_string(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat() 
    return data

def run_infinite_post_data_loop():
    records_to_send = 500  
    record_count = 0

    while record_count < records_to_send:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Select random rows from the tables
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_result = convert_datetime_to_string(pin_result)  

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result = convert_datetime_to_string(geo_result)  

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result = convert_datetime_to_string(user_result) 
            
            send_to_kafka(KAFKA_TOPICS["pin_data"], pin_result)
            send_to_kafka(KAFKA_TOPICS["geo_data"], geo_result)
            send_to_kafka(KAFKA_TOPICS["user_data"], user_result)

            record_count += 1  
            print(f"Sent {record_count} records to Kafka topics")
            
        sleep(random.uniform(0.5, 2))

    print(f"Finished sending {records_to_send} records to Kafka topics.")

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
