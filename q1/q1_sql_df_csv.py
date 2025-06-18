from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TaxiDataAnalysisDF_CSV") \
    .getOrCreate()

# Start time measurement
start_time = time.time()

# Load CSV data
csv_path = "hdfs:///user/alopix/processed/yellow_tripdata_2015-01.csv"
df_csv = spark.read.csv(csv_path, header=True, inferSchema=True)

# Process data
result_csv = df_csv.filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0)) \
    .withColumn("hour", hour("tpep_pickup_datetime")) \
    .groupBy("hour") \
    .agg(
        avg("pickup_longitude").alias("avg_longitude"),
        avg("pickup_latitude").alias("avg_latitude")
    ) \
    .orderBy("hour")

# Show results
result_csv.show(24)

# End time measurement
end_time = time.time()
print(f"Execution time with DataFrame API (CSV): {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()