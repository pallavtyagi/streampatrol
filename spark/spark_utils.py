from pyspark.sql import SparkSession


class SparkUtils:
    def __init__(self, app_name, delta_checkpoint_location, delta_table_location):
        self.app_name = app_name
        self.delta_checkpoint_location = delta_checkpoint_location
        self.delta_table_location = delta_table_location
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        return SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    def write_to_delta(self, df):
        return df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", self.delta_checkpoint_location) \
            .queryName(self.app_name) \
            .start(self.delta_table_location)

    def get_spark_session(self):
        return self.spark
