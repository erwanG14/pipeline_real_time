import os

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType,BooleanType , TimestampType
from pyspark.sql.functions import col, from_json, window,to_timestamp

ready_file = "/opt/spark/work-dir/status/consumer_ready"
if os.path.exists(ready_file):
    os.remove(ready_file) 
lunar_distance_km = 384000

spark = SparkSession.builder\
    .appName("KafkaStream")\
    .master("local[*]")\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
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
  .option("subscribe", "test_topic") \
  .load()

schema = StructType()\
    .add("name_limited",StringType()) \
    .add("designation",StringType()) \
    .add("asteroid_full_name",StringType()) \
    .add("neo_reference_id",StringType()) \
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



df_KM = df.drop('velocity_km_h','miss_distance_lunar','miss_distance_au')

schema_KM_col_double = [
    field.name
    for field in df_KM.schema.fields
    if isinstance(field.dataType, DoubleType)
]

df_grouped = df_KM\
    .withWatermark("timestamp_event", "5 seconds")\
    .groupBy(
        window("timestamp_event", "5 seconds"),
        "asteroid_full_name","hazardous_flag"
    ).agg(
        *[F.avg(col).alias(col+"_avg")
        for col in schema_KM_col_double
        ]
    )

df_kpi_size = df_grouped.withColumn(
    "Size_kpi",
    F.when(col("diameter_avg_km_avg") < 0.05, "tiny")
    .when(col("diameter_avg_km_avg") < 0.2,"small")
    .when(col("diameter_avg_km_avg") < 1,"medium")
    .otherwise("large")
)
df_kpi_mach = df_kpi_size.withColumn(
    "mach_kpi",
    (col("velocity_km_s_avg")*3600)/1224,
)

df_risk = df_kpi_mach.withColumn(
    "risk_score",
    (
        F.when(col("hazardous_flag") == True, 50).otherwise(0)
        + F.when(col("diameter_avg_km_avg")<1, 20).otherwise(0)
        + F.when(col("miss_distance_km_avg") < lunar_distance_km, 40)
          .when(col("miss_distance_km_avg") < lunar_distance_km * 5, 25)
          .when(col("miss_distance_km_avg") < lunar_distance_km *20, 10)
          .otherwise(0)
        + F.when(col("mach_kpi") > 5, 15).otherwise(0)
    )
)
df_risk_clean=(df_risk\
    .withColumn("window_start",col("window.start"))\
    .withColumn("window_end",col("window.end"))\
    .drop("window")
)


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
         
         
         
         

