from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV_to_Parquet_Converter") \
    .getOrCreate()

files = [
    ("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv", "taxi_zone_lookup"),
    ("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv", "yellow_tripdata_2015"),
    ("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv", "yellow_tripdata_2024")
]
output_base_path = "hdfs://hdfs-namenode:9000/user/akoukosias/data/parquet/"

for input_path, output_folder in files:
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    output_path = output_base_path + output_folder

    df.write.mode("overwrite").parquet(output_path)

    print(f"Αποθηκεύτηκε στο: {output_path}")

spark.stop()
