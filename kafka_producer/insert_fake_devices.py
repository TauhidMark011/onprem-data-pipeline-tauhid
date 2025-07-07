import faker
import random
import mysql.connector
import time

fake = faker.Faker()

#MySQL connection
conn = mysql.connector.connect(
    host='mysql',
    port=3306,
    user='root',
    password='root',
    database='weather_pipeline'
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS devices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        device_id VARCHAR(50),
        device_type VARCHAR(50),
        status VARCHAR(10),
        location VARCHAR(100)
    )
""")

#Insert fake data
device_types = ['Thermometer', 'Barometer', 'Hygrometer']
statuses = ['active', 'inactive']

for _ in range(10):
    device_id = fake.uuid4()
    device_type = random.choice(device_types)
    status = random.choice(statuses)
    # location = fake.city()
    location = "Delhi"

    cursor.execute("""
        INSERT INTO devices (device_id, device_type, status, location)
        VALUES (%s, %s, %s, %s)
    """, (device_id, device_type, status, location))

    print(f"Inserted: {device_id}, {device_type}, {status}, {location}")
    time.sleep(1)

conn.commit()
cursor.close()
conn.close()
