from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, MapType
from spark_utils import SparkUtils

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'spark-stream-monitor'

# Initialize SparkUtils
spark_utils = SparkUtils(
    app_name="kafka_to_delta_stream",
    delta_checkpoint_location="./lakehouse/checkpoints/kafka_to_delta_stream/",
    delta_table_location="./lakehouse/tables/kafka_to_delta_stream/"
)

# Get Spark session
spark = spark_utils.get_spark_session()

# Define the schema for the JSON data
json_schema = StructType([
    StructField("event", StringType(), True),
    StructField("id", StringType(), True),
    StructField("runId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("batchId", LongType(), True),
    StructField("numInputRows", LongType(), True),
    StructField("inputRowsPerSecond", DoubleType(), True),
    StructField("processedRowsPerSecond", DoubleType(), True),
    StructField("durationMs", MapType(StringType(), LongType()), True),
    StructField("stateOperators", StringType(), True),
    StructField("sources", StringType(), True),
    StructField("sink", StringType(), True)
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Select the value column and cast it to string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

# Parse the JSON string and extract keys as columns
parsed_df = kafka_df.withColumn("parsed_value", from_json(col("json_value"), json_schema)).select("parsed_value.*")

# Write to Delta table
query = spark_utils.write_to_delta(parsed_df)

query.awaitTermination()