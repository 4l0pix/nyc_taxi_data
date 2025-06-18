"""
RUNS BUT DOESNT FINISH: EXITS WITH ERROR
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from math import radians, sin, cos, sqrt, atan2, asin
import time

# Haversine UDF function
def haversine_distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return c * 6371  # Earth radius in km

# Register UDF
haversine_udf = udf(haversine_distance, DoubleType())

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TaxiMaxDistanceDF_Parquet") \
    .getOrCreate()

# Start time measurement
start_time = time.time()

# Load Parquet data
parquet_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015" 
df_parquet = spark.read.parquet(parquet_path)

# Calculate distance and duration
result_parquet = df_parquet.filter(
    (col("pickup_longitude") != 0) & (col("pickup_latitude") != 0) &
    (col("dropoff_longitude") != 0) & (col("dropoff_latitude") != 0)
).withColumn(
    "distance",
    haversine_udf(
        col("pickup_longitude"), col("pickup_latitude"),
        col("dropoff_longitude"), col("dropoff_latitude")
    )
).withColumn(
    "duration",
    (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60
)

# Find maximum distance per vendor with its duration
max_distance_parquet = result_parquet.groupBy("VendorID") \
    .agg(
        {"distance": "max", "duration": "first"}
    ) \
    .select(
        col("VendorID"),
        col("max(distance)").alias("max_distance_km"),
        col("first(duration)").alias("trip_duration_min")
    ) \
    .orderBy("VendorID")

# Show results
max_distance_parquet.show()

# End time measurement
end_time = time.time()
print(f"Execution time with DataFrame API (Parquet): {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()
