import os
from pyspark.sql import SparkSession, functions as F
import findspark
from pyspark.sql.types import *
findspark.init("<directory_of_your_spark_home>")

# Create spark session
spark = (SparkSession.builder
         .appName("Edit data and write to disc")
         #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .master("local[2]")
         .getOrCreate())

# Get all room number as a list
room_list = os.listdir("<directory_of_your_KETI_folder>")
room_list.remove("README.txt")
csv_list = ["co2.csv", "humidity.csv", "light.csv", "pir.csv", "temperature.csv"]
# print(room_list)

# Create schema for an empty dataframe.
schema_join = StructType([StructField('ts_min_bignt', IntegerType(), True),
                          StructField("room", StringType(), True)])

# Create an empty dataframe.
df_joined = spark.createDataFrame([], schema_join)

for csv in csv_list:
    value = csv.split(".")[0]
    # print(value)
    # Create another schema for empty dataframe to union all rooms data.
    schema = StructType([
        StructField('ts_min_bignt', IntegerType(), True),
        StructField(f"{value}", DoubleType(), True),
        StructField("room", StringType(), True),
    ])
    # Create the other empty dataframe. This will be created again for every measurement value in for loop.
    df_all_room = spark.createDataFrame([], schema)
    for room in room_list:
        path = f"file:///<directory_of_your_KETI_folder>/{room}/{csv}" # home/user/.../KETI
        df_per_room = spark.read.format("csv").load(path).withColumn("room", F.lit(room))
        # Use union to add rows/dataframes of each room.
        df_all_room = df_all_room.union(df_per_room)

    # Join all dataframes for each measurement value based on ts_min_bignt and room
    df_joined = df_joined.join(df_all_room, ["ts_min_bignt", "room"], "outer")

# Add event_ts_min column that is created from ts_min_bignt as expected.
df_final = df_joined.withColumn("event_ts_min", F.from_unixtime(F.col("ts_min_bignt")))

# Write data to as file.
df_final.coalesce(1).write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", True) \
        .save("file:///<directory_of_the_folder_to_save>") # home/user/.../folder_name

