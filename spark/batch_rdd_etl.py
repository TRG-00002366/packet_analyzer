import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date
from utils.utils import ones_complement_checksum
from utils.snowflake import build_snowflake_options


RAW_PATH = "/app/data/raw/"
OUTPUT_PATH = "/app/data/transformed/rdd_packets_per_dst_ip"
SNOWFLAKE_STAGE_TABLE = os.getenv("SNOWFLAKE_STAGING_TABLE", "STG_PACKET_EVENTS")


def expected_checksum(packet_row):
    header_fields = {
        "packet_id": str(packet_row.packet_id),
        "version": packet_row.version,
        "data_length": packet_row.data_length,
        "protocol": packet_row.protocol,
        "src_ip": packet_row.src_ip,
        "dst_ip": packet_row.dst_ip,
    }
    header_str = json.dumps(header_fields, sort_keys=True)
    return ones_complement_checksum(header_str)


def write_safe_packets_to_snowflake(packet_df):
    snowflake_options = build_snowflake_options(SNOWFLAKE_STAGE_TABLE)

    packet_df.select(
        col("packet_id").alias("PACKET_ID"),
        col("version").alias("VERSION"),
        col("ip_header_length").alias("IP_HEADER_LENGTH"),
        col("data_length").alias("DATA_LENGTH"),
        col("protocol").alias("PROTOCOL"),
        col("checksum").alias("CHECKSUM"),
        col("src_ip").alias("SRC_IP"),
        col("dst_ip").alias("DST_IP"),
        col("src_port").alias("SRC_PORT"),
        col("dest_port").alias("DEST_PORT"),
        col("control_flags").alias("CONTROL_FLAGS"),
        col("window_size").alias("WINDOW_SIZE"),
        col("data").alias("DATA"),
        col("timestamp").alias("EVENT_EPOCH"),
        col("event_date").alias("EVENT_DATE"),
        col("event_time").alias("EVENT_TIME"),
    ).write \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .mode("append") \
        .save()

spark = SparkSession.builder \
    .appName("PacketBatchRDD") \
    .getOrCreate()

sc = spark.sparkContext

# Read raw parquet
df = spark.read.parquet(RAW_PATH)
if "event_time" not in df.columns:
    df = df.withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))
if "event_date" not in df.columns:
    df = df.withColumn("event_date", to_date(col("event_time")))

packet_schema = df.schema
rdd = df.rdd
total_packets = rdd.count()

#Filter out LoopBack address
no_loopback_rdd = rdd.filter(
    lambda x: x.src_ip != "127.0.0.1" and x.dst_ip != "127.0.0.1"
).cache()
no_loopback_packets = no_loopback_rdd.count()

#Filter out incorrect checksums

verified_checksum_rdd = no_loopback_rdd.filter(
    lambda x: expected_checksum(x) == x.checksum
).cache()
verified_checksum_packets = verified_checksum_rdd.count()

#Filter out packets that are on  the blacklist
try:
    with open("/app/data/blacklist.txt", 'r', encoding="utf-8") as file:
        blacklist_broadcast = sc.broadcast([line for line in file.read().splitlines() if line])
except FileNotFoundError:
    print("Error: The file blacklist.txt was not found.")
    blacklist_broadcast = sc.broadcast([])

safe_packets_rdd = verified_checksum_rdd.filter(
    lambda x: x.src_ip not in blacklist_broadcast.value and x.dst_ip not in blacklist_broadcast.value
).cache()
safe_packets = safe_packets_rdd.count()

print(f"Total packets read: {total_packets}")
print(f"Filtered loopback packets: {total_packets - no_loopback_packets}")
print(f"Filtered invalid checksum packets: {no_loopback_packets - verified_checksum_packets}")
print(f"Filtered blacklisted packets: {verified_checksum_packets - safe_packets}")
print(f"Packets remaining after filtering: {safe_packets}")

packets_per_dst_ip = safe_packets_rdd \
    .map(lambda x: (x.dst_ip, 1)) \
    .reduceByKey(lambda left, right: left + right) \
    .sortBy(lambda x: x[1], ascending=False)


summary_df = packets_per_dst_ip.toDF(["dst_ip", "total"])
summary_df.show(5)
summary_df.write.mode("overwrite").csv(OUTPUT_PATH, header=True)

safe_packets_df = spark.createDataFrame(safe_packets_rdd, schema=packet_schema)
write_safe_packets_to_snowflake(safe_packets_df)

spark.stop()