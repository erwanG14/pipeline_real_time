import os
import writer
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType,BooleanType , TimestampType
from pyspark.sql.functions import col, from_json, window,to_timestamp,round

ready_file = "/opt/spark/work-dir/status/consumer_ready"
"""if os.path.exists(ready_file):
    os.remove(ready_file) """

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




"""
def write_table(batch_df, batch_id):
    df_dashboard = batch_df\
    .withColumn(
    "distance_lunar_equivalent",
    col("miss_distance_km_avg") / F.lit(lunar_distance_km)
).withColumn(
    "is_close_approach",
    col("miss_distance_km_avg") < F.lit(lunar_distance_km * 5)
).select(
    "window_start",
    "window_end",
    "asteroid_full_name",
    "hazardous_flag",
    "risk_score",
    "size_kpi",
    "mach_kpi",
    "velocity_km_s_avg",
    "miss_distance_km_avg",
    "distance_lunar_equivalent",
    "is_close_approach"
)
    df_dashboard = df_dashboard.dropDuplicates([
        "asteroid_full_name",
        "risk_score",
        "size_kpi",
        "mach_kpi",
        "velocity_km_s_avg"
    ])
    batch_df.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/asteroids_db") \
        .option("dbtable", "asteroid_risk_raw") \
        .option("user", "spark") \
        .option("password", "spark") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    df_dashboard.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/asteroids_db") \
        .option("dbtable", "asteroid_risk_dashboard_raw") \
        .option("user", "spark") \
        .option("password", "spark") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = df_risk_clean.writeStream\
         .foreachBatch(write_table)\
         .outputMode("update")\
         .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/asteroid_dashboard") \
         .start()
open(ready_file, "w").close()
try:
    query.awaitTermination()
finally:
    if os.path.exists(ready_file):
        os.remove(ready_file)
         
"""
         
         

