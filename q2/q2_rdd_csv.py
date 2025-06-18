from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import time
from math import radians, sin, cos, sqrt, atan2, asin
from datetime import datetime

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

# Initialize Spark
conf = SparkConf().setAppName("TaxiMaxDistanceRDD")
sc = SparkContext(conf=conf)

# Start time measurement
start_time = time.time()

# Load the CSV file from HDFS
input_path = "hdfs:///user/alopix/processed/yellow_tripdata_2015-01.csv"
taxi_data = sc.textFile(input_path)

# Remove header and filter out invalid records
header = taxi_data.first()
taxi_data = taxi_data.filter(lambda line: line != header) \
    .map(lambda line: line.split(",")) \
    .filter(lambda fields: len(fields) >= 19) \
    .filter(lambda fields: fields[5] != '0' and fields[6] != '0' and  # pickup coords not zero
                          fields[9] != '0' and fields[10] != '0')  # dropoff coords not zero

# Parse vendor, coords, and timestamps
vendor_distance = taxi_data.map(lambda fields: (
    fields[0],  # vendor_id
    (
        haversine(
            float(fields[5]), float(fields[6]),  # pickup_lon, pickup_lat
            float(fields[9]), float(fields[10])   # dropoff_lon, dropoff_lat
        ),
        (
            datetime.strptime(fields[2], "%Y-%m-%d %H:%M:%S") -  # dropoff_time
            datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")    # pickup_time
        ).total_seconds() / 60  # duration in minutes
    )
))

# Find maximum distance per vendor with its duration
max_distance = vendor_distance.reduceByKey(
    lambda x, y: x if x[0] > y[0] else y
).sortByKey()

# Collect results
results = max_distance.collect()

# Print results
print("Vendor\tMax Distance (km)\tTrip Duration (min)")
for vendor, (distance, duration) in results:
    print(f"{vendor}\t{distance:.2f}\t\t{duration:.2f}")

# End time measurement
end_time = time.time()
print(f"Execution time with RDD API: {end_time - start_time:.2f} seconds")

# Stop Spark context
sc.stop()