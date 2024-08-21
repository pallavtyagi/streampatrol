import random
import string
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from spark_utils import SparkUtils
from monitoring.stream_monitor import CustomStreamingQueryListener

KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'spark-stream-monitor'


def generate_random_string(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def main():
    random_string_udf = udf(generate_random_string, StringType())

    spark_utils = SparkUtils(
        app_name="streaming_demo_app",
        delta_checkpoint_location="./lakehouse/checkpoints/random_string_stream/",
        delta_table_location="./lakehouse/tables/random_string_stream/"
    )

    spark = spark_utils.get_spark_session()
    listener = CustomStreamingQueryListener(KAFKA_SERVER, KAFKA_TOPIC)
    streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    random_string_df = streaming_df.withColumn("random_string", random_string_udf())
    spark.streams.addListener(listener)

    query = spark_utils.write_to_delta(random_string_df)
    query.awaitTermination()


if __name__ == "__main__":
    main()
