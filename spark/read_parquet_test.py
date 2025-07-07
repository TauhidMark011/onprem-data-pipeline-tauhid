from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquetTest") \
    .getOrCreate()

df = spark.read.parquet("/opt/weather_parquet/")
df.printSchema()
df.show(5, truncate=False)

spark.stop()