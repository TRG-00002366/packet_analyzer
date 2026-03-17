from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

spark = SparkSession.builder \
    .appName("PacketBatchDF") \
    .getOrCreate()

# Read raw stream output
df = spark.read.parquet("data/raw/")

df.show()

spark.stop()