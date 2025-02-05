import requests
import json
import random
import time
import sqlalchemy
from sqlalchemy import create_engine, text
import yaml
from datetime import datetime

random.seed(100)

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


# Function to send data to the streaming API
def send_to_stream(stream_name, data, partition_key):  
    
    invoke_url = f"https://2s1rfz0va3.execute-api.us-east-1.amazonaws.com/dev/streams/Kinesis-Prod-Stream/record"

    payload = json.dumps({
        "StreamName": "Kinesis-Prod-Stream",
        "Data": data,
        "PartitionKey": partition_key
    })
    
    headers = {
        "Content-Type": "application/json",
    }

    try:
        response = requests.request("PUT", invoke_url, headers=headers, data=payload)
        if response.status_code == 200:
            print(f"Data sent to {partition_key} successfully")
        else:
            print(f"Failed to send data to {partition_key}, Status Code: {response.status_code}, Response: {response.json()}")
    except Exception as e:
        print(f"Error sending data to {partition_key}: {e}")

def convert_datetime_to_string(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat() 
    return data

# Function to fetch data from the database
def fetch_data_from_db(random_row):
    engine = new_connector.create_db_connector()

    with engine.connect() as connection:
        # Select random rows from the tables
        pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        pin_selected_row = connection.execute(pin_string)
        pin_result = None
        for row in pin_selected_row:
            pin_result = dict(row._mapping)
            pin_result = convert_datetime_to_string(pin_result)  

        geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
        geo_selected_row = connection.execute(geo_string)
        geo_result = None
        for row in geo_selected_row:
            geo_result = dict(row._mapping)
            geo_result = convert_datetime_to_string(geo_result)  

        user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
        user_selected_row = connection.execute(user_string)
        user_result = None
        for row in user_selected_row:
            user_result = dict(row._mapping)
            user_result = convert_datetime_to_string(user_result)

    return pin_result, geo_result, user_result

# Function to send data in a loop to the streaming API
def run_infinite_post_data_loop():
    records_to_send = 500  # Adjust this value as needed
    record_count = 0

    while record_count < records_to_send:
        time.sleep(random.uniform(0.5, 2))  # Adjust sleep time as needed
        random_row = random.randint(0, 11000)

        pin_result, geo_result, user_result = fetch_data_from_db(random_row)
        
        if pin_result:
            send_to_stream("Kinesis-Prod-Stream", pin_result, "pinterest_data")
        if geo_result:
            send_to_stream("Kinesis-Prod-Stream", geo_result, "geolocation_data")
        if user_result:
            send_to_stream("Kinesis-Prod-Stream", user_result, "user_data")
        
        record_count += 1
        print(f"Sent {record_count} records to the streaming API")

    print(f"Finished sending {records_to_send} records to the streaming API.")

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Emulation of user posting complete.')