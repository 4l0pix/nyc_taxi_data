from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxiSameBoroughDF_Parquet") \
    .getOrCreate()

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_2024 = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015" 
)

taxi_zones = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup"
)

# Create aliases
pickup_zones = taxi_zones.alias("pickup_zones")
dropoff_zones = taxi_zones.alias("dropoff_zones")

# Join with borough information
trips_with_boroughs = trips_2024.join(
    pickup_zones,
    trips_2024.PULocationID == col("pickup_zones.LocationID"),
    "left"
).join(
    dropoff_zones,
    trips_2024.DOLocationID == col("dropoff_zones.LocationID"),
    "left"
).select(
    col("pickup_zones.Borough").alias("PickupBorough"),
    col("dropoff_zones.Borough").alias("DropoffBorough")
)

# Filter and count
borough_counts = trips_with_boroughs.filter(
    col("PickupBorough") == col("DropoffBorough")
).groupBy("PickupBorough") \
 .agg(count("*").alias("TotalTrips")) \
 .orderBy(col("TotalTrips").desc())

# Show results
borough_counts.show()

# End time measurement
end_time = time.time()
print(f"Execution time with DataFrame API (Parquet): {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()
