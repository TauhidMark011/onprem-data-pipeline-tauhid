#Created a Kafka topic (e.g., weather-topic)
#A Python script to:
#Fetch weather data from OpenWeatherMap API
#Push it to Kafka topic
from kafka import KafkaProducer
import requests
import json
import time
import logging
from dotenv import load_dotenv
import os

# ------------------------ Logger Setup ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ------------------------ API Setup ------------------------
load_dotenv(dotenv_path="/opt/airflow/kafka_producer/.env")
API_KEY = os.getenv("API_KEY")
CITY = 'Delhi'
URL = f'https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric'

# ------------------------ Kafka Producer ------------------------
try:
    producer = KafkaProducer(
        bootstrap_servers='host.docker.internal:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("✅ Kafka producer initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Kafka producer: {e}")
    exit(1)

# ------------------------ Kafka Callbacks ------------------------
def on_success(record_metadata):    
    logging.info(
        f"Sent to topic '{record_metadata.topic}' | Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
    )

def on_error(excp):
    logging.error(f"Failed to send message to Kafka: {excp}")

# ------------------------ Main Loop ------------------------
while True:
    try:
        logging.info(f"Fetching weather data for {CITY}...")
        response = requests.get(URL)
        response.raise_for_status()  # Raise error for bad responses

        data = response.json()
        weather_data = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "city": CITY,
            "latitude": data["coord"]["lat"],        #NEW
            "longitude": data["coord"]["lon"],   
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"]["speed"],
            "description": data["weather"][0]["description"]
        }

        logging.info(f"Sending weather data to Kafka: {weather_data}")
        producer.send('weather-topic', weather_data).add_callback(on_success).add_errback(on_error)
        producer.flush()

    except requests.exceptions.RequestException as req_err:
        logging.error(f"API Request error: {req_err}")
    except KeyError as key_err:
        logging.error(f"Key error in JSON response: {key_err}")
    except Exception as e:
        logging.error(f"Unexpected error in producer loop: {e}")

    time.sleep(60)  # Wait 1 minute before next fetch