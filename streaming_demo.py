import random
import string
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.streaming import StreamingContext

from stream_monitor import CustomStreamingQueryListener


# Function to generate random strings
def generate_random_string(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


randon_string_udf = udf(generate_random_string, StringType())

spark = SparkSession.builder \
    .appName("streaming_demo_app") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0rc1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'spark-stream-monitor'

listener = CustomStreamingQueryListener(kafka_bootstrap_servers, kafka_topic)
spark.streams.addListener(listener)

streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

randon_string_df = streaming_df.withColumn("random_string", randon_string_udf())

query = randon_string_df.writeStream \
    .queryName("random_string_stream") \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "./lakehouse/checkpoints/randon_string_stream/") \
    .start("./lakehouse/tables/randon_string_stream/")

# query = randon_string_df.writeStream.queryName("random_string_stream").outputMode("append").format("console").start()
query.awaitTermination()

spark.streams.removeListener(listener)
