import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, rank, dense_rank, 
    hour, from_unixtime, date_format, window,
    when, lit, broadcast, countDistinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

spark = SparkSession.builder \
    .appName("PacketBatchDF") \
    .getOrCreate()

# Read raw stream output
# df = spark.read.parquet("data/raw/")
data = [
    ("10001", 4, 20, 256, "TCP", 25622, "127.0.0.1", "127.0.0.1", 443, 8080, "SYN", 321, "GET /index.html", 1772396701),
    ("10002", 6, 40, 512, "UDP", 5672, "10.0.0.2", "192.168.1.1", 53, 53, "ACK", 654, "", 1772396702),
    ("10003", 4, 24, 128, "TCP", 11613, "172.16.0.1", "8.8.8.8", 80, 12345, "SYN", 564, "POST /login", 1772396703),
    ("10004", 4, 16, 64, "UDP", 55895, "192.168.1.20", "10.0.0.8", 5000, 5001, "FIN", 111, "data", 1772396704),
    ("10005", 6, 32, 1024, "TCP", 59681, "10.0.0.3", "192.168.1.2", 22, 22, "SYN",  222, "", 1772396705),
    ("10006", 4, 20, 128, "TCP", 8674, "192.168.1.30", "10.0.0.9", 8080, 443, "ACK",  333, "HEAD /", 1772396706),
    ("10007", 4, 16, 256, "UDP", 3617, "10.0.0.4", "192.168.1.3", 67, 68, "SYN", 512,  "", 1772396707),
    ("10008", 6, 40, 512, "TCP", 11363, "172.16.0.2", "172.67.112.241", 25, 110, "FIN", 1024,  "MAIL FROM:", 1772396708),
    ("10009", 4, 24, 128, "UDP", 60899, "172.67.112.241", "10.0.0.10", 161, 162, "ACK",  666, "", 1772396709),
    ("10010", 4, 16, 64, "TCP", 190, "10.0.0.5", "172.67.112.241", 3306, 3306, "SYN", 777, "SELECT *", 1772396710),
]

df = spark.createDataFrame(data, [
    "packet_id", "version", "ip_header_length", "data_length", "protocol", 
    "checksum", "src_ip", "dst_ip", "src_port", "dest_port", 
    "control_flags", "window_size", "data", "timestamp"
])

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
time.sleep(3)


# # Write hourly summary to Parquet
# hourly_summary.write \
#     .mode("overwrite") \
#     .partitionBy("hour") \
#     .parquet("data/transformed/hourly_summary")

# Transformation 2: Top 10 DST_IP by packet count
print("\n=== 2. Top 10 Destination IPs ===")
dst_ip_window = Window.orderBy(col("packet_count").desc())

top_dst_ips = df.groupBy("dst_ip") \
    .agg(count("*").alias("packet_count")) \
    .withColumn("rank", rank().over(dst_ip_window)) \
    .filter(col("rank") <= 10) \
    .orderBy("rank")
print("Top 10 Destination IPs")
top_dst_ips.show()
time.sleep(3)

# # Write top DST IPs to Parquet
# top_dst_ips.write \
#     .mode("overwrite") \
#     .parquet("data/transformed/top_dst_ips")

# Transformation 3: Department Traffic (if departments.csv exists)
# print("\n=== 3. Department Traffic Analysis ===")
# try:
#     departments_df = spark.read.csv("data/departments.csv", header=True, inferSchema=True)
    
#     # Join packets with departments (assuming departments.csv has ip_address, department columns)
#     dept_traffic = df.join(
#         broadcast(departments_df),
#         (df["src_ip"] == departments_df["ip_address"]) | 
#         (df["dst_ip"] == departments_df["ip_address"]),
#         "left_outer"
#     ).groupBy(
#         when(df["src_ip"] == departments_df["ip_address"], departments_df["department"])
#         .when(df["dst_ip"] == departments_df["ip_address"], departments_df["department"])
#         .otherwise("Unknown").alias("department")
#     ).agg(
#         count("*").alias("total_packets"),
#         countDistinct("src_ip").alias("unique_sources"),
#         countDistinct("dst_ip").alias("unique_destinations"),
#         avg("data_length").alias("avg_packet_size")
#     ).orderBy(col("total_packets").desc())
    
#     dept_traffic.show()
    
#     # Write department traffic to Parquet
#     dept_traffic.write \
#         .mode("overwrite") \
#         .partitionBy("department") \
#         .parquet("data/transformed/department_traffic")
        
# except Exception as e:
#     print(f"Department analysis skipped - departments.csv not found: {e}")
#     # Create basic IP traffic analysis without departments
#     ip_traffic = df.groupBy("src_ip", "dst_ip") \
#         .agg(
#             count("*").alias("packet_count"),
#             avg("data_length").alias("avg_packet_size"),
#             countDistinct("protocol").alias("protocol_count")
#         ).orderBy(col("packet_count").desc())
    
#     ip_traffic.show(20)
    
#     # Write IP traffic to Parquet
#     ip_traffic.write \
#         .mode("overwrite") \
#         .parquet("data/transformed/ip_traffic")

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
time.sleep(3)

# Write SYN flood detection results to Parquet
# syn_traffic.write \
#     .mode("overwrite") \
#     .partitionBy("dst_ip") \
#     .parquet("data/transformed/syn_flood_detection")

# Additional analytics: Protocol distribution
print("\n=== Protocol Distribution ===")
protocol_dist = df.groupBy("protocol") \
    .agg(
        count("*").alias("packet_count"),
        avg("data_length").alias("avg_size")
    ).orderBy(col("packet_count").desc())

print("Protocol Distribution")
protocol_dist.show()
time.sleep(3)

# Write protocol distribution to Parquet
# protocol_dist.write \
#     .mode("overwrite") \
#     .parquet("data/transformed/protocol_distribution")

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