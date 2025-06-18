from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TaxiDataAnalysisDF_Parquet") \
    .getOrCreate()

# Start time measurement
start_time = time.time()

# Load Parquet data
parquet_path = "hdfs:///user/alopix/processed/yellow_trip_data_2015-01.parquet"
df_parquet = spark.read.parquet(parquet_path)

# Process data
result_parquet = df_parquet.filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0)) \
    .withColumn("hour", hour("tpep_pickup_datetime")) \
    .groupBy("hour") \
    .agg(
        avg("pickup_longitude").alias("avg_longitude"),
        avg("pickup_latitude").alias("avg_latitude")
    ) \
    .orderBy("hour")

# Show results
result_parquet.show(24)

# End time measurement
end_time = time.time()
print(f"Execution time with DataFrame API (Parquet): {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()