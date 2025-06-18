from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxiRevenueByBoroughDF_Parquet") \
    .getOrCreate()

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_2024 = spark.read.parquet(
    "hdfs:///user/alopix/processed/yellow_tripdata_2024-01.parquet"
)

taxi_zones = spark.read.parquet(
    "hdfs:///user/alopix/processed/taxi_zone_lookup.parquet"
)

# Join with borough information
borough_revenue = trips_2024.join(
    taxi_zones,
    trips_2024.PULocationID == taxi_zones.LocationID,
    "left"
).groupBy("Borough") \
.agg(
    sum("fare_amount").alias("total_fare"),
    sum("tip_amount").alias("total_tip"),
    sum("tolls_amount").alias("total_tolls"),
    sum("extra").alias("total_extra"),
    sum("mta_tax").alias("total_mta_tax"),
    sum("congestion_surcharge").alias("total_congestion"),
    sum("airport_fee").alias("total_airport_fee"),
    sum("total_amount").alias("grand_total")
).orderBy(desc("grand_total"))

# Show results
borough_revenue.show(truncate=False)

# End time measurement
end_time = time.time()
print(f"Execution time with DataFrame API (Parquet): {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()
