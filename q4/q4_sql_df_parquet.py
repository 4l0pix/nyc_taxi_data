from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, hour
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxiNightTripsDF_Parquet") \
    .getOrCreate()

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_2024 = spark.read.parquet(
    "hdfs:///user/alopix/processed/yellow_tripdata_2024-01.parquet"
)

# Filter night trips (23:00-23:59 or 00:00-06:59)
night_trips = trips_2024.filter(
    (hour(col("tpep_pickup_datetime")) == 23) |
    (hour(col("tpep_pickup_datetime")).between(0, 6))
)

# Count trips by vendor
vendor_counts = night_trips.groupBy("VendorID") \
    .agg(count("*").alias("NightTripsCount")) \
    .orderBy(col("NightTripsCount").desc())

# Show results
vendor_counts.show()

# End time measurement
end_time = time.time()
print(f"Execution time with DataFrame API (Parquet): {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()
