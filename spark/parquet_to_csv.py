from pyspark.sql import SparkSession

#Start Spark session
spark = SparkSession.builder \
    .appName("Parquet to CSV Converter") \
    .getOrCreate()

#Path to Parquet files (already mounted to your container!)
parquet_path = "/opt/weather_parquet/"

#Read all part-*.parquet files
df = spark.read.parquet(parquet_path)

#Show sample records in the terminal
print("Showing sample data from Parquet:")
df.show(truncate=False)

#Save to CSV
output_path = "/opt/spark-app/weather_csv"
df.write.mode("overwrite").option("header", True).csv(output_path)

print("✅ CSV saved at:", output_path)

#Stop Spark session
spark.stop()
