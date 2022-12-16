from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Create spark session with elasticsearch and kafka jar packages
spark = (SparkSession.builder
         .appName("streaming_data_processing")
         .master("local[2]")
         .config("spark.jars.packages",
                 "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1,"
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# Read messages from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-input") \
    .load()

# Operation : Convert encrypted messages to appropriate format
# - Kafka keeps messages serialized. Deserialization is required.
cast_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")

# - One row of the dataset is kept in the value part of the kafka message. We should split "value" and take sub-values.
formatted_df = cast_df.withColumn("ts_min_bignt", F.split(F.col("value"), ",")[0].cast(IntegerType())) \
    .withColumn("room", F.split(F.col("value"), ",")[1].cast(StringType())) \
    .withColumn("co2", F.split(F.col("value"), ",")[2].cast(DoubleType())) \
    .withColumn("humidity", F.split(F.col("value"), ",")[3].cast(DoubleType())) \
    .withColumn("light", F.split(F.col("value"), ",")[4].cast(DoubleType())) \
    .withColumn("pir", F.split(F.col("value"), ",")[5].cast(DoubleType())) \
    .withColumn("temp", F.split(F.col("value"), ",")[6].cast(DoubleType())) \
    .withColumn("event_ts_min", F.split(F.col("value"), ",")[7].cast(TimestampType())) \

stream_df = formatted_df.select("ts_min_bignt", "room", "co2", "humidity", "light", "pir", "temp", "event_ts_min")

checkpointDir = "file:///tmp/streaming/file_source_sensor"

sensor_input_map = {
    "mappings": {
        "properties": {
            "ts_min_bignt": {"type": "integer"},
            "room": {"type": "keyword"},
            "co2": {"type": "float"},
            "humidity": {"type": "float"},
            "light": {"type": "float"},
            "pir": {"type": "float"},
            "temp": {"type": "float"},
            "event_ts_min": {"type": "date"},
        }
    }
}

# Create elasticsearch client.
es = Elasticsearch("http://localhost:9200")


try:
    # Delete the index if exist.
    es.indices.delete("smart-building-sensor")
    print("smart-building-sensor index deleted.")
except:
    print("No index")

# Create the index using map.
es.indices.create(index="smart-building-sensor", body=sensor_input_map)

# Write data to elasticsearch. You can use the "console" format to see the data before writing it to Elasticsearch.
streamingQuery = (stream_df
                  .writeStream
                  # .format("console")
                  .format("org.elasticsearch.spark.sql")
                  .outputMode("append")
                  .trigger(processingTime="5 second")
                  .option("numRows", 10)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpointDir)
                  .option("es.resource", "smart-building-sensor")
                  .option("es.nodes", "localhost:9200")
                  .start())

streamingQuery.awaitTermination()

# # Read from elasticsearch if you want to check database from here.
# df_es = spark.read \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.nodes", "localhost") \
#     .option("es.port","9200") \
#     .load("smart-building-sensor")
#
# print(df_es.show(20))
# print(df_es.printSchema())