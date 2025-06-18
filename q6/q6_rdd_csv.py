from pyspark import SparkContext, SparkConf
import time

# Initialize Spark
conf = SparkConf().setAppName("NYCTaxiRevenueByBoroughRDD")
sc = SparkContext(conf=conf)

# Start time measurement
start_time = time.time()

# Load data from HDFS
trips_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
zones_path = "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv"

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
    .filter(lambda fields: len(fields) >= 19)  # Ensure we have all required fields

# Map to (PULocationID, payment_components)
def parse_payments(fields):
    try:
        pulocation = int(fields[7])
        components = (
            float(fields[12]),  # fare_amount
            float(fields[15]),  # tip_amount
            float(fields[16]),  # tolls_amount
            float(fields[13]),  # extra
            float(fields[14]),  # mta_tax
            float(fields[18]),  # congestion_surcharge
            float(fields[19]) if len(fields) > 19 and fields[19] else 0.0,  # airport_fee
            float(fields[17])  # total_amount
        )
        return (pulocation, components)
    except:
        return None

payments_by_location = trips_rdd.map(parse_payments).filter(lambda x: x is not None)

# Aggregate by borough
def aggregate_payments(acc, new):
    return tuple(a + n for a, n in zip(acc, new))

borough_revenue = payments_by_location \
    .map(lambda x: (zones_broadcast.value.get(x[0], "Unknown"), x[1])) \
    .aggregateByKey(
        (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),  # Initial values
        aggregate_payments,  # Sequence operation
        aggregate_payments   # Combination operation
    ) \
    .sortBy(lambda x: -x[1][7])  # Sort by total_amount descending

# Collect and print results
results = borough_revenue.collect()
print("Borough\t\tFare\tTip\tTolls\tExtra\tMTA Tax\tCongestion\tAirport\tTotal")
for borough, (fare, tip, tolls, extra, mta, congestion, airport, total) in results:
    print(f"{borough[:12]}\t{fare:.2f}\t{tip:.2f}\t{tolls:.2f}\t{extra:.2f}\t{mta:.2f}\t{congestion:.2f}\t\t{airport:.2f}\t{total:.2f}")

# End time measurement
end_time = time.time()
print(f"Execution time with RDD API: {end_time - start_time:.2f} seconds")

# Stop Spark context
sc.stop()
