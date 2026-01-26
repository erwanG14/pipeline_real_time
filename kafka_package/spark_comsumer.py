import json
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType,BooleanType , TimestampType
from pyspark.sql.functions import col, from_json, window

spark = SparkSession.builder\
    .appName("KafkaStream")\
    .master("local[*]")\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"
    )\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df_raw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
  .option("subscribe", "test_topic") \
  .load()

schema = StructType()\
    .add("name_limited",StringType()) \
    .add("designation",StringType()) \
    .add("asteroid_full_name",StringType()) \
    .add("neo_reference_id",IntegerType()) \
    .add("hazardous_flag",BooleanType()) \
    .add("sentry_flag",BooleanType()) \
    .add("diameter_min_km",DoubleType()) \
    .add("diameter_max_km",DoubleType()) \
    .add("diameter_avg_km",DoubleType()) \
    .add("orbit_class_type",StringType()) \
    .add("orbit_class_description",StringType()) \
    .add("eccentricity",DoubleType()) \
    .add("semi_major_axis_au",DoubleType()) \
    .add("inclination_deg",DoubleType()) \
    .add("orbital_period_days",DoubleType()) \
    .add("perihelion_distance_au",DoubleType()) \
    .add("orbit_uncertainty",StringType()) \
    .add("event_id",StringType()) \
    .add("approach_date",StringType()) \
    .add("approach_date_full",StringType()) \
    .add("orbiting_body",StringType()) \
    .add("velocity_km_s",DoubleType()) \
    .add("velocity_km_h",DoubleType()) \
    .add("miss_distance_km",DoubleType()) \
    .add("miss_distance_lunar",DoubleType()) \
    .add("miss_distance_au",DoubleType()) \
    .add("timestamp_event",TimestampType()) 


df = df_raw\
    .selectExpr("CAST(value AS STRING) as json")\
    .select(from_json(col("json"), schema).alias("data"))\
    .select("data.*")

df_grouped = df\
    .withWatermark("timestamp_event", "5 seconds")\
    .groupBy(
        window("timestamp_event", "5 seconds"),
        "asteroid_full_name"
    )\
    .avg()

query = df_grouped.writeStream\
         .format("console")\
         .option("truncate" ,"false")\
         .start()
query.awaitTermination()
         
         
         
         

