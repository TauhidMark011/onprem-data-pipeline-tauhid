#Using the Faker Python library to generate fake weather logs and save them into a CSV file every minute.
import csv
import os
import time
from faker import Faker
from datetime import datetime
import random

#Initialize Faker
fake = Faker()

#Create output directory if not exists
output_dir = "kafka_producer/weather_logs"
os.makedirs(output_dir, exist_ok=True)

def generate_fake_weather_row():
    return {
        "name": fake.name(),
        "city": fake.city(),
        "temperature": round(random.uniform(25.0, 45.0), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def write_to_csv():
    while True:
        filename = f"{output_dir}/weather_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=["name", "city", "temperature", "timestamp"])
            writer.writeheader()
            for _ in range(5):  #write 5 fake rows per file
                writer.writerow(generate_fake_weather_row())

        print(f"Written: {filename}")
        time.sleep(60)  #wait 1 minute

if __name__ == "__main__":
    write_to_csv()
