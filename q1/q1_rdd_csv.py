from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import time
from datetime import datetime

# Initialize Spark
conf = SparkConf().setAppName("TaxiDataAnalysisRDD")
sc = SparkContext(conf=conf)

# Start time measurement
start_time = time.time()

# Load the CSV file from HDFS
input_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
taxi_data = sc.textFile(input_path)

# Remove header and filter out records with zero coordinates
header = taxi_data.first()
taxi_data = taxi_data.filter(lambda line: line != header) \
    .map(lambda line: line.split(",")) \
    .filter(lambda fields: len(fields) >= 6) \
    .filter(lambda fields: fields[5] != '0' and fields[6] != '0')  # pickup_longitude and pickup_latitude

# Extract hour, longitude, latitude
hour_coords = taxi_data.map(lambda fields: (
    datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S").hour if fields[1] else None,
    (float(fields[5]), float(fields[6]))  # (longitude, latitude)
)).filter(lambda x: x[0] is not None)

# Calculate average coordinates per hour
avg_coords = hour_coords.groupByKey() \
    .mapValues(lambda coords: (
        sum(lon for lon, lat in coords) / len(coords),
        sum(lat for lon, lat in coords) / len(coords)
    )) \
    .sortByKey()

# Collect results
results = avg_coords.collect()

# Print results
print("Hour\tAvg Longitude\tAvg Latitude")
for hour, (avg_lon, avg_lat) in results:
    print(f"{hour}\t{avg_lon:.6f}\t{avg_lat:.6f}")

# End time measurement
end_time = time.time()
print(f"Execution time with RDD API: {end_time - start_time:.2f} seconds")

# Stop Spark context
sc.stop()
