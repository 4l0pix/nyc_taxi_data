from pyspark import SparkContext, SparkConf
from datetime import datetime
import time

# Initialize Spark
conf = SparkConf().setAppName("NYCTaxiNightTripsRDD")
sc = SparkContext(conf=conf)

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",

# Load and process trips data
trips_header = sc.textFile(trips_path).first()
trips_rdd = sc.textFile(trips_path) \
    .filter(lambda line: line != trips_header) \
    .map(lambda line: line.split(",")) \
    .filter(lambda fields: len(fields) >= 2)  # Ensure we have VendorID and tpep_pickup_datetime

# Function to check if time is between 23:00-23:59 or 00:00-06:59
def is_night_time(dt_str):
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        hour = dt.hour
        return hour == 23 or 0 <= hour <= 6
    except:
        return False

# Filter night trips and map to (VendorID, 1)
night_trips = trips_rdd.filter(lambda fields: is_night_time(fields[1])) \
    .map(lambda fields: (fields[0], 1))  # (VendorID, 1)

# Count trips by vendor
vendor_counts = night_trips.reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: -x[1])  # Sort descending by count

# Collect results
results = vendor_counts.collect()

# Print results
print("VendorID\tNight Trips Count")
for vendor, count in results:
    print(f"{vendor}\t\t{count}")

# End time measurement
end_time = time.time()
print(f"Execution time with RDD API: {end_time - start_time:.2f} seconds")

# Stop Spark context
sc.stop()
