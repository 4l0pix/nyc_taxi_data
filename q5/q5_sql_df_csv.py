from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxiTopZonePairsDF_CSV") \
    .getOrCreate()

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_2024 = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
    header=True,
    inferSchema=True
)

taxi_zones = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
)

# Create aliases
pickup_zones = taxi_zones.alias("pickup_zones")
dropoff_zones = taxi_zones.alias("dropoff_zones")

# Join with zone information and filter out same-zone trips
zone_pairs = trips_2024.join(
    pickup_zones,
    trips_2024.PULocationID == col("pickup_zones.LocationID"),
    "left"
).join(
    dropoff_zones,
    trips_2024.DOLocationID == col("dropoff_zones.LocationID"),
    "left"
).filter(
    col("pickup_zones.LocationID") != col("dropoff_zones.LocationID")
).select(
    col("pickup_zones.Zone").alias("PickupZone"),
    col("dropoff_zones.Zone").alias("DropoffZone")
)

# Count trips by zone pairs
zone_pair_counts = zone_pairs.groupBy("PickupZone", "DropoffZone") \
    .agg(count("*").alias("TripCount")) \
    .orderBy(desc("TripCount"))

# Show top 20 results
zone_pair_counts.show(20, truncate=False)

# End time measurement
end_time = time.time()
print(f"Execution time with DataFrame API (CSV): {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()
