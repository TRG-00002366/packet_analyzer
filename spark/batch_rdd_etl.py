from pyspark.sql import SparkSession
from utils.utils import ones_complement_checksum

spark = SparkSession.builder \
    .appName("PacketBatchRDD") \
    .getOrCreate()

sc = spark.sparkContext

# Read raw parquet
df = spark.read.parquet("data/raw/")
rdd = df.rdd


#Filter out LoopBack address
no_loopback_rdd = rdd.filter(lambda x: x[6] != "127.0.0.1" and x[7] != "127.0.0.1")


#Filter out incorrect checksums

verified_checksum_rdd = no_loopback_rdd.filter(
    lambda x: ones_complement_checksum(','.join(str(f) for f in x[:5] + x[6:8])) == x[5]
)

#Filter out packets that are on  the blacklist
try:
    with open("/app/data/blacklist.txt", 'r') as file:
        blacklist = file.read().split("\n")
        sc.broadcast(blacklist)
except FileNotFoundError:
    print("Error: The file blacklist.txt was not found.")

safe_packets_rdd = verified_checksum_rdd.filter(lambda x: (x[6] not in blacklist) and (x[7] not in blacklist))

#Total packets per destination IP


print(safe_packets_rdd.collect())

spark.stop()