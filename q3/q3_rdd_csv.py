from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import time

# Initialize Spark
conf = SparkConf().setAppName("NYCTaxiSameBoroughRDD")
sc = SparkContext(conf=conf)

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_path = "hdfs:///user/alopix/processed/yellow-tripdata-2024-01.csv"
zones_path = "hdfs:///user/alopix/processed/taxi_zone_lookup.csv"

# Load and process taxi zones
zones_header = sc.textFile(zones_path).first()
zones_rdd = sc.textFile(zones_path) \
    .filter(lambda line: line != zones_header) \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), fields[1]))  # (LocationID, Borough)

# Create broadcast variable for zones
zones_dict = dict(zones_rdd.collect())
zones_broadcast = sc.broadcast(zones_dict)

# Load and process trips data
trips_header = sc.textFile(trips_path).first()
trips_rdd = sc.textFile(trips_path) \
    .filter(lambda line: line != trips_header) \
    .map(lambda line: line.split(",")) \
    .filter(lambda fields: len(fields) >= 8)  # Ensure we have PULocationID and DOLocationID

# Map to (PULocationID, DOLocationID) pairs
location_pairs = trips_rdd.map(lambda fields: (
    int(fields[7]),  # PULocationID
    int(fields[8])   # DOLocationID
))

# Filter trips where pickup and dropoff are in same borough
same_borough_trips = location_pairs.filter(lambda x: (
    x[0] in zones_broadcast.value and
    x[1] in zones_broadcast.value and
    zones_broadcast.value[x[0]] == zones_broadcast.value[x[1]]
))

# Count by borough
borough_counts = same_borough_trips.map(lambda x: (zones_broadcast.value[x[0]], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: -x[1])  # Sort descending by count

# Collect results
results = borough_counts.collect()

# Print results
print("Borough\tTotal Trips")
for borough, count in results:
    print(f"{borough}\t{count}")

# End time measurement
end_time = time.time()
print(f"Execution time with RDD API: {end_time - start_time:.2f} seconds")

# Stop Spark context
sc.stop()