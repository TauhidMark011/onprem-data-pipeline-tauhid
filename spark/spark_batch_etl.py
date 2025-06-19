from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Create Spark Session with Hive Metastore URI
spark = SparkSession.builder \
    .appName("Spark Batch ETL - Weather + Devices") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.catalog.clearCache()  #Clear old file references
print("✅ Spark Session Created Successfully")

# Read weather logs
weather_df = spark.read.option("header", "true") \
    .csv("/home/talam/weather_input/")

# Cast temperature to double
weather_df = weather_df.withColumn("temperature", col("temperature").cast("double"))

# Read devices table from MySQL
jdbc_url = "jdbc:mysql://localhost:3307/weather_pipeline"
jdbc_props = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

devices_df = spark.read.jdbc(
    url=jdbc_url,
    table="devices",
    properties=jdbc_props
)

# Corrected Join: city from weather_df matches location in devices_df
joined_df = weather_df.join(
    devices_df,
    weather_df["city"] == devices_df["location"],
    "inner"
)

# Filter active devices
filtered_df = joined_df.filter(col("status") == "active")

# Select only the necessary final columns (and rename timestamp → log_time)
final_df = filtered_df.select(
    "device_id", "location", "temperature", "timestamp"
).withColumnRenamed("timestamp", "log_time")

# Write to Hive
final_df.write.mode("overwrite") \
    .format("parquet") \
    .saveAsTable("final_table")

# Write to MySQL
final_df.write.jdbc(
    url=jdbc_url,
    table="final_table",
    mode="overwrite",
    properties=jdbc_props
)
print("✅ Written to MySQL Table: final_table")
print("✅ ETL Job Completed")
