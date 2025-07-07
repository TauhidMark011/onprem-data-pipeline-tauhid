#Spark Streaming job script that reads data from Kafka and writes it somewhere (either console or Parquet).
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

#Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

print("✅ Spark session created successfully.")

#Define Kafka source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✅ Connected to Kafka topic: weather-topic")

#Define schema for JSON
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("city", StringType()) \
    .add("latitude", FloatType()) \
    .add("longitude", FloatType()) \
    .add("temperature", FloatType()) \
    .add("humidity", FloatType()) \
    .add("pressure", FloatType()) \
    .add("wind_speed", FloatType()) \
    .add("description", StringType())

#Parse Kafka message value as JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
#Adding these logs here
json_df.printSchema()
print("JSON parsing complete.")
#Debug Console Sink Here (non-blocking)
json_df.select("latitude", "longitude") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
#Write to Parquet in micro-batches
query = json_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/weather_parquet/") \
    .option("checkpointLocation", "/opt/weather_parquet/_checkpoint/") \
    .outputMode("append") \
    .trigger(processingTime="5 minute") \
    .start()

print("✅ Streaming query started. Writing to /opt/weather_parquet/")
#Await Termination on Main Query
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n Stopping the streaming query gracefully...")
    query.stop()  # Stop the query
    spark.stop()  # Optional: Stop Spark session
    print("Stream stopped successfully.")
    
