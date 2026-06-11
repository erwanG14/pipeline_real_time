import os
import writer
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType,BooleanType , TimestampType
from pyspark.sql.functions import col, from_json, window,to_timestamp,round

spark = SparkSession.builder\
    .appName("KafkaStream")\
    .master("local[*]")\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config("spark.sql.shuffle.partitions", "2") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.3"
    )\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df_raw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "airplane_topic") \
  .option("startingOffsets", "latest") \
  .load() #stocker data en tant que bronze

schema = StructType()\
    .add("hex",StringType()) \
    .add("type",StringType()) \
    .add("flight",StringType()) \
    .add("r",StringType()) \
    .add("t",StringType()) \
    .add("desc",StringType()) \
    .add("lat",DoubleType()) \
    .add("lon",DoubleType()) \
    .add("alt_geom",IntegerType()) \
    .add("alt_baro",IntegerType()) \
    .add("gs",DoubleType()) \
    .add("track",DoubleType()) \
    .add("geom_rate",IntegerType()) \
    .add("mach",DoubleType()) \
    .add("squawk",IntegerType()) \
    .add("emergency",StringType()) \
    .add("category",StringType()) \
    .add("messages",IntegerType()) \
    .add("alert",IntegerType()) \
    .add("seen",DoubleType()) \
    .add("seen_pos",DoubleType()) \
    .add("timestamp_ingest",TimestampType())

df = df_raw\
    .selectExpr("CAST(value AS STRING) as json")\
    .select(from_json(col("json"), schema).alias("data"))\
    .select("data.*")

df_km = df.withColumns({
    "gs_km_h" : round(col("gs") * 1.852,2),
    "alt_baro_km" : round(col("alt_baro") * 0.0003048,2),
    "alt_geom_km" : round(col("alt_geom") * 0.0003048,2)
    })

df_silver = df_km.withColumn(
    "KPI_speed_type",
    F.when(col("gs_km_h") < 200, "slow")
     .when((col("gs_km_h") >= 200) & (col("gs_km_h") < 500), "medium")
     .when((col("gs_km_h") >= 500) & (col("gs_km_h") < 700), "fast")
     .when(col("gs_km_h") >= 700, "really-fast")
     .otherwise("unknown")
    )

query = df_silver.writeStream \
    .foreachBatch(writer.write_all_tables) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/airplane_pipeline_v1") \
    .start()

query.awaitTermination()
         
         

