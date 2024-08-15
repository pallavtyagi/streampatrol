from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure Spark session to use Delta Lake and Kafka
spark = SparkSession.builder \
    .appName("kafka_to_delta_stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-spark_2.12:3.0.0rc1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'spark-stream-monitor'

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Select the value column and cast it to string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Write to Delta table
query = kafka_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "./lakehouse/checkpoints/kafka_to_delta_stream/") \
    .start("./lakehouse/tables/kafka_to_delta_stream/")

# query = kafka_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()


query.awaitTermination()