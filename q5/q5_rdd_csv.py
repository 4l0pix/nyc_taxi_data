from pyspark import SparkContext, SparkConf
import time

# Initialize Spark
conf = SparkConf().setAppName("NYCTaxiTopZonePairsRDD")
sc = SparkContext(conf=conf)

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_path = "hdfs:///user/alopix/processed/yellow_tripdata_2024-01.csv"
zones_path = "hdfs:///user/alopix/processed/taxi_zone_lookup.csv"

# Load and process taxi zones
zones_header = sc.textFile(zones_path).first()
zones_rdd = sc.textFile(zones_path) \
    .filter(lambda line: line != zones_header) \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (int(fields[0]), fields[1]))  # (LocationID, Zone)

# Create broadcast variable for zones
zones_dict = dict(zones_rdd.collect())
zones_broadcast = sc.broadcast(zones_dict)

# Load and process trips data
trips_header = sc.textFile(trips_path).first()
trips_rdd = sc.textFile(trips_path) \
    .filter(lambda line: line != trips_header) \
    .map(lambda line: line.split(",")) \
    .filter(lambda fields: len(fields) >= 8)  # Ensure we have PULocationID and DOLocationID

# Map to (PULocationID, DOLocationID) pairs and filter out same-zone trips
zone_pairs = trips_rdd.map(lambda fields: (
    int(fields[7]),  # PULocationID
    int(fields[8])   # DOLocationID
)).filter(lambda x: x[0] != x[1])

# Count trips by zone pairs
zone_pair_counts = zone_pairs.map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: -x[1])  # Sort descending by count

# Map to zone names and take top 20
top_zone_pairs = zone_pair_counts.map(lambda x: (
    zones_broadcast.value.get(x[0][0], "Unknown"),
    zones_broadcast.value.get(x[0][1], "Unknown"),
    x[1]
)).take(20)

# Print results
print("Pickup Zone\tDropoff Zone\tTrip Count")
for pickup, dropoff, count in top_zone_pairs:
    print(f"{pickup}\t{dropoff}\t{count}")

# End time measurement
end_time = time.time()
print(f"Execution time with RDD API: {end_time - start_time:.2f} seconds")

# Stop Spark context
sc.stop()
