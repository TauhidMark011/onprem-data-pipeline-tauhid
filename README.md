## Stable Release

* Current Stable Tag: *`stable-pipeline-v1`*
* First fully integrated working version of the on-premise data pipeline
* Includes Docker Containers, Airflow orchestration, Kafka ingestion, Spark batch processing, MySQL storage, Hive, Prometheus monitoring, and Grafana dashboards.
- This release represents the first end-to-end validated pipeline execution with successful DAG runs and monitoring enabled.

<img width="1536" height="1024" alt="ChatGPT Image Jul 3, 2025, 09_35_11 PM" src="https://github.com/user-attachments/assets/4f2a2140-74e8-4c88-8d2e-f18f575b35bd" />

<img width="1366" height="768" alt="Screenshot (1052)" src="https://github.com/user-attachments/assets/aec0ac36-0c3a-4be8-a020-82b2920b9a40" />

<img width="1366" height="768" alt="Screenshot (1055)" src="https://github.com/user-attachments/assets/adcb9f3a-8160-4aa2-bac8-cd678a7b57b1" />

<img width="1366" height="768" alt="Screenshot (1035)" src="https://github.com/user-attachments/assets/f065bc16-0db3-41bd-872d-c97226ec333b" />

<img width="1366" height="768" alt="Screenshot (1053)" src="https://github.com/user-attachments/assets/9b66bd4f-3210-439e-b1bc-70e1f8221fee" />

<img width="1366" height="768" alt="Screenshot (1054)" src="https://github.com/user-attachments/assets/f7c9dc92-54d8-4b98-911e-2ebc24c0cd82" />

<img width="1366" height="768" alt="Screenshot (1058)" src="https://github.com/user-attachments/assets/f0f269a5-c13f-4924-b641-7aa21f601a3b" />

<img width="1366" height="768" alt="Screenshot (1059)" src="https://github.com/user-attachments/assets/f310042f-0c82-465c-9ff7-4dfe0b556ac7" />

# onprem-data-pipeline-tauhid
This project demonstrates a complete Full-Stack On-Premise Data Pipeline built for processing real-time and batch data from IoT and Weather sources. It integrates key big data components including MySQL, Kafka, Spark (Batch & Streaming), Hive, Airflow, and Docker, showcasing practical end-to-end data engineering skills.

📌 **Overview**
This project demonstrates the end-to-end implementation of a Full-Stack On-Premise Data Pipeline for ingesting, processing, storing, and visualizing IoT device data along with real-time weather updates.
Designed for Data Engineers and Big Data Enthusiasts, it showcases:
Streaming data ingestion with Apache Kafka,
Batch ETL with Apache Spark,
Integration of API and synthetic data sources,
Hybrid storage in Hive and MySQL,
Workflow orchestration with Apache Airflow,
Monitoring via Grafana & Prometheus.

🚀__Features__
**Multi-source Ingestion**
- Real-time Weather API ingestion (OpenWeatherMap)
- Synthetic IoT device logs with Faker Python library
- Direct MySQL insertions for sensor/device data
  
**Streaming & Batch Processing**
- Stream weather data into Kafka topics
- Store Kafka-processed data into Parquet
- Batch ETL to merge weather + IoT data in Spark
  
**Hybrid Storage**
- Persist batch results in Hive Warehouse
- Load final results into MySQL

**Automation & Scheduling**
- Orchestrate ingestion and ETL jobs with Airflow DAGs

**Monitoring**
- Optional visualization with Grafana dashboards

 __Tech Stack__ :- 
 
 | Layer              | Technology                                      |
| ------------------ | ----------------------------------------------- |
| **Data Ingestion** | Python, OpenWeatherMap API, Faker, Apache Kafka |
| **Message Broker** | Apache Kafka + Zookeeper                        |
| **Processing**     | Apache Spark (Streaming + Batch)                |
| **Storage**        | Apache Hive, MySQL, Parquet                     |
| **Orchestration**  | Apache Airflow                                  |
| **Monitoring**     | Grafana, Prometheus                             |
| **Environment**    | Docker, Docker Compose                          |
| **Dev Tools**      | VS Code, DBeaver, MySQL Workbench               |

**Architecture**

                 +-----------------------+
-                 |   OpenWeather API      |
                 +-----------+-----------+
                             |
                             v
    +------------+      +-----------------+     +------------+
-   |   Faker    | ---> |   Kafka Topic   | <-- | Weather API |
    +------------+      +--------+--------+     +------------+
                             |
                             v
                    +-------------------+
 -                   | Spark Streaming   |
                    |  (Write to Parquet)|
                    +--------+----------+
                             |
                 +-----------+------------+
                 | Spark Batch Processing |
                 | (Join CSV + MySQL)     |
                 +-----------+------------+
                             |
         +-------------------+--------------------+
         |                                        |
    +--------v--------+                     +--------v--------+
-   |  Hive Table     |              -       | MySQL Final Table |
    +-----------------+                     +-----------------+

**Modules**
3.1 **Ingestion**
- Weather API → Kafka
- Faker → CSV
- MySQL Sensor Data Injection

**3.2 Processing**
- Spark Streaming → Parquet
- Spark Batch ETL → Join CSV + MySQL → Hive & MySQL

**3.3 Storage**
- Parquet files for raw processed data
- Hive final table for analytics
- MySQL final table for BI tools

**3.4 Orchestration & Monitoring**
- Apache Airflow DAGs to schedule ingestion + batch jobs
- Grafana Dashboards for health & performance metrics

__Installation & Setup__
1️⃣ Clone the Repository
git clone https://github.com/<your-username>/fullstack-onpremise-data-pipeline.git
cd fullstack-onpremise-data-pipeline

2️⃣ Start Docker Services
docker-compose up -d --build

3️⃣ Configure Python Environment
pip install -r requirements.txt

4️⃣ Run Ingestion Scripts
python kafka_producer/weather_api_producer.py
python kafka_producer/generate_fake_weather_csv.py
python kafka_producer/insert_fake_devices.py

📊 **Example Outputs**
- Parquet Sample
timestamp, city, temperature, humidity, pressure, wind_speed, description
2025-05-29 18:22:29, Delhi, 41.16, 17.0, 991.0, 2.03, few clouds

- MySQL Devices Table
| id | device_id | device_type | status | location |
|-----|-----------|-------------|----------|----------|
| 1 | 39ab64e7 | Thermometer | inactive | Delhi |

 **Use Cases**
- IoT Sensor Data Pipelines
- Real-time & Batch Data Engineering
- Big Data Workflow Orchestration
- Hybrid Data Lake + Data Warehouse Architectures

  📌 Future Enhancements
- Add Machine Learning model for weather prediction
- Integrate BI dashboards with Power BI/Tableau
- Expand to Kubernetes-based deployment.

  *📄License
- This project is licensed under the MIT License - see the LICENSE file for details.

#Designed as part of a Data Engineering Bootcamp (GUVI - DE-WE-E-B2), this project reflects real-world architecture and orchestration patterns, making it valuable for production-level pipeline design and cloud migration readiness.
