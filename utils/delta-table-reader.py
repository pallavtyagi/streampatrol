from prettytable import PrettyTable
from spark.spark_utils import SparkUtils

spark_utils = SparkUtils(
    app_name="delta_table_reader",
    delta_checkpoint_location="./lakehouse/checkpoints/delta_table_reader/",
    delta_table_location="./lakehouse/tables/kafka_to_delta_stream/"
)

spark = spark_utils.get_spark_session()

delta_table_path = "./lakehouse/tables/kafka_to_delta_stream/"

delta_df = spark.read.format("delta").load(delta_table_path)

pandas_df = delta_df.toPandas()

table = PrettyTable()

table.field_names = pandas_df.columns.tolist()
for row in pandas_df.itertuples(index=False):
    table.add_row(row)

print(table)