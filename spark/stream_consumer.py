from pyspark.sql import SparkSession

def consumer(bootstrap_servers = "kafka:9092", output_path = "data/raw"):
    # Create Spark session with Kafka support
    spark = SparkSession.builder \
        .appName("KafkaSparkIntegration") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # Read from Kafka topic as a streaming DataFrame
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "packets") \
        .load()

    # The value column contains the actual message (as bytes)
    messages = kafka_df.selectExpr("CAST(value AS STRING) as message")

    #Output as parquet
    query = messages.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", output_path + "/_checkpoints") \
        .start()

    query.awaitTermination()



print("Starting Kafka Spark consumer...")
consumer()