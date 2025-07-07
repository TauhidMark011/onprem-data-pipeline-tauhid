from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Create Spark Session with Hive Metastore URI
spark = SparkSession.builder \
    .appName("Spark Batch ETL - Weather + Devices") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.catalog.clearCache()
print("✅ Spark Session Created Successfully")

# Setup logger for Airflow or stdout
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# 1. Read Weather Data from Parquet
weather_df = spark.read.parquet("/opt/weather_parquet/")
# weather_df = weather_df.limit(100)  # Optional limit for debugging

logger.info("Weather Schema:")
weather_df.printSchema()
logger.info("✅ Sample Weather Data:")
weather_df.show(5, truncate=False)

# Ensure correct type
weather_df = weather_df.withColumn("temperature", col("temperature").cast("double"))

# 2. MySQL JDBC Properties
jdbc_url = "jdbc:mysql://mysql:3306/weather_pipeline?allowPublicKeyRetrieval=true&useSSL=false"
jdbc_props = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}
# 3. Read Devices Table from MySQL
devices_df = spark.read.jdbc(
    url=jdbc_url,
    table="devices",
    properties=jdbc_props
)
# devices_df = devices_df.limit(10)  # Optional limit for testing

# 4. Join weather & devices on location = city
joined_df = weather_df.join(
    devices_df,
    weather_df["city"] == devices_df["location"],
    "inner"
)
print(f"Joined Rows: {joined_df.count()}")

# 5. Filter Active Devices
filtered_df = joined_df.filter(col("status") == "active")

# 6. Select & Rename Columns
final_df = filtered_df.select(
    "device_id", "location", "temperature", "latitude", "longitude", "timestamp"
).withColumnRenamed("timestamp", "log_time")

# 7. Drop Duplicates if any
final_df = final_df.dropDuplicates(["device_id", "log_time"])

# # 8. Write to Hive Table (optional for this project)
# final_df.write.mode("overwrite") \
#     .format("parquet") \
#     .saveAsTable("final_table")

# 9. Write to MySQL Table with Safety
try:
    final_df.write.jdbc(
        url=jdbc_url,
        table="final_table",
        mode="overwrite",
        properties=jdbc_props
    )
    print("✅ Written to MySQL Table: final_table")
except Exception as e:
    print(f"Failed to write to MySQL: {e}")

print("✅ ETL Job Completed")
