from pyspark.sql import SparkSession
import pandas as pd
from prettytable import PrettyTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("delta_table_reader") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0rc1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to the Delta table
delta_table_path = "./lakehouse/tables/kafka_to_delta_stream/"

# Read Delta table
delta_df = spark.read.format("delta").load(delta_table_path)

# Convert to Pandas DataFrame
pandas_df = delta_df.toPandas()

# Create PrettyTable
table = PrettyTable()

# Add columns
table.field_names = pandas_df.columns.tolist()
for row in pandas_df.itertuples(index=False):
    table.add_row(row)

# Print PrettyTable
print(table)