from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, rank, dense_rank, 
    hour, from_unixtime, date_format, window,
    when, lit, broadcast, countDistinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

RAW_PATH = "/app/data/raw/"
OUTPUT_PATH = "/app/data/transformed/protocol_distribution"


spark = SparkSession.builder \
    .appName("PacketBatchDF") \
    .getOrCreate()

# Read raw stream output
df = spark.read.parquet(RAW_PATH)

# Cache the base DataFrame for performance
df.cache()

# Create timestamp column from Unix timestamp
df = df.withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))

print("=== Raw Data Schema ===")
df.printSchema()
print(f"Total packets: {df.count()}")

# Transformation 1: Hourly packet summary
print("\n=== 1. Hourly Packet Summary ===")
hourly_summary = df.groupBy(
    date_format(col("event_time"), "yyyy-MM-dd HH").alias("hour")
).agg(
    count("*").alias("total_packets"),
    countDistinct("src_ip").alias("total_src_ip_packets"),
    countDistinct("dst_ip").alias("total_dst_ip_packets"),
    avg("data_length").alias("avg_data_length")
).orderBy("hour")

print("Hourly Packet Summary")
hourly_summary.show()



print("\n=== 2. Top 10 Destination IPs ===")
dst_ip_window = Window.orderBy(col("packet_count").desc())

top_dst_ips = df.groupBy("dst_ip") \
    .agg(count("*").alias("packet_count")) \
    .withColumn("rank", rank().over(dst_ip_window)) \
    .filter(col("rank") <= 10) \
    .orderBy("rank")
print("Top 10 Destination IPs")
top_dst_ips.show()


# Transformation 4: SYN Flood Detection
print("\n=== 4. SYN Flood Detection ===")
syn_traffic = df.filter(col("control_flags") == "SYN") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        "dst_ip"
    ).agg(
        count("*").alias("syn_packet_count"),
        countDistinct("src_ip").alias("unique_sources"),
        count("*").alias("total_packets")
    ).filter(col("syn_packet_count") > 100)  \
    .orderBy(col("syn_packet_count").desc())
print("SYN Flood Detection")
syn_traffic.show()


print("\n=== Protocol Distribution ===")
protocol_dist = df.groupBy("protocol") \
    .agg(
        count("*").alias("packet_count"),
        avg("data_length").alias("avg_size")
    ).orderBy(col("packet_count").desc())

print("Protocol Distribution")
protocol_dist.show()


# Write protocol distribution to Parquet
protocol_dist.write.mode("overwrite").csv(OUTPUT_PATH, header=True)

print("\n=== Transformation Summary ===")
try:
    print(f"Hourly summaries: {hourly_summary.count()} hours")
except:
    print("Hourly summaries: Not generated")

try:
    print(f"Top destination IPs: {top_dst_ips.count()} IPs")
except:
    print("Top destination IPs: Not generated")

try:
    print(f"SYN flood alerts: {syn_traffic.count()} potential attacks")
except:
    print("SYN flood alerts: Not generated")

# Uncache the DataFrame
df.unpersist()

spark.stop()