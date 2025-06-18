from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi Q3 - Trips Within Same Borough (CSV)") \
    .getOrCreate()

# Load 2024 trip data (CSV)
trips_2024 = spark.read.csv(
    "/home/alopix/nyc-taxi-analysis/data/raw/yellow-tripdata-2024.csv",
    header=True,
    inferSchema=True
)

# Load taxi zone lookup (CSV)
taxi_zones = spark.read.csv(
    "/home/alopix/nyc-taxi-analysis/data/raw/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
)

# Create aliases for the taxi_zones DataFrame
pickup_zones = taxi_zones.alias("pickup_zones")
dropoff_zones = taxi_zones.alias("dropoff_zones")

# Join with explicit column references
trips_with_boroughs = trips_2024.join(
    pickup_zones,
    trips_2024.PULocationID == col("pickup_zones.LocationID"),
    "left"
).join(
    dropoff_zones,
    trips_2024.DOLocationID == col("dropoff_zones.LocationID"),
    "left"
).select(
    trips_2024["*"],
    col("pickup_zones.Borough").alias("PickupBorough"),
    col("dropoff_zones.Borough").alias("DropoffBorough")
)

# Filter trips where pickup and dropoff are in the same borough
same_borough_trips = trips_with_boroughs.filter(
    col("PickupBorough") == col("DropoffBorough")
)

# Count trips by borough
borough_counts = same_borough_trips.groupBy("PickupBorough") \
    .agg(count("*").alias("TotalTrips")) \
    .orderBy(col("TotalTrips").desc())

# Show results
borough_counts.show()




spark.stop()
