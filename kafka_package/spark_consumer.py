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
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.postgresql:postgresql:42.7.3"
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
    .add("alert",StringType()) \
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

query_silver = writer.write_stream(
    df_silver,
    "airplane_silver",
    "/opt/spark/work-dir/checkpoints/airplane_silver_v2"
    )

df_gold_total = df_silver\
    .withWatermark("timestamp_ingest", "5 seconds")\
    .groupBy(
        window(col("timestamp_ingest"),"10 seconds")
        )\
    .agg(
        F.approx_count_distinct("hex").alias("nb_plane"),
        F.round(F.avg("gs_km_h"),2).alias("average_ground_speed_km_h"),
        F.round(F.avg("alt_geom_km"),2).alias("average_altitude_geom_km"),
        F.sum(col("alert")).alias("nb_alert")
    )

df_gold_total = df_gold_total.select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    F.col("nb_plane"),
    F.col("average_ground_speed_km_h"),
    F.col("average_altitude_geom_km"),
    F.col("nb_alert")
)

query_gold_total = writer.write_stream(
    df_gold_total,
    "airplane_gold_total",
    "/opt/spark/work-dir/checkpoints/airplane_gold_total_v2"
    )

df_gold_KPI_speed = df_silver\
    .withWatermark("timestamp_ingest", "5 seconds")\
    .groupBy(
        window(col("timestamp_ingest"),"10 seconds"),
        "KPI_speed_type"
        )\
    .agg(
        F.approx_count_distinct("hex").alias("nb_plane_per_speed_type"),
        F.round(F.avg("gs_km_h"),2).alias("average_ground_speed_km_h_per_speed_type"),
        F.round(F.avg("alt_geom_km"),2).alias("average_altitude_geom_km_per_speed_type"),
        F.sum(col("alert")).alias("nb_alert_per_speed_type")
        )

df_gold_KPI_speed = df_gold_KPI_speed.select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    F.col("KPI_speed_type"),
    F.col("nb_plane_per_speed_type"),
    F.col("average_ground_speed_km_h_per_speed_type"),
    F.col("average_altitude_geom_km_per_speed_type"),
    F.col("nb_alert_per_speed_type")
)

query_gold_KPI_speed = writer.write_stream(
    df_gold_KPI_speed,
    "airplane_gold_kpi_speed",
    "/opt/spark/work-dir/checkpoints/airplane_gold_kpi_speed_v2"
    )

spark.streams.awaitAnyTermination()



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
         
         

