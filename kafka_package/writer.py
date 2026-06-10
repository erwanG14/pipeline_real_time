from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, window,to_timestamp,round

def write_table(batch_df,batch_id,table_name):
    batch_df.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airplane_db") \
        .option("dbtable", table_name) \
        .option("user", "spark") \
        .option("password", "spark") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
def write_stream(df,table_name,checkpoint_location):
    return df.writeStream\
         .outputMode("append")\
         .trigger(processingTime="10 seconds") \
         .foreachBatch(lambda batch_df, batch_id: write_table(
                batch_df,
                batch_id,
                table_name
          ))\
         .option("checkpointLocation", checkpoint_location)\
         .start()
def write_batch(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airplane_db") \
        .option("dbtable", table_name) \
        .option("user", "spark") \
        .option("password", "spark") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
def write_all_tables(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df = batch_df.persist()

    # SILVER
    write_batch(
        batch_df,
        "airplane_silver"
    )

    # GOLD TOTAL
    df_gold_total = batch_df \
        .groupBy(
            F.window(col("timestamp_ingest"), "10 seconds")
        ) \
        .agg(
            F.approx_count_distinct("hex").alias("nb_plane"),
            F.round(F.avg("gs_km_h"), 2).alias("average_ground_speed_km_h"),
            F.round(F.avg("alt_geom_km"), 2).alias("average_altitude_geom_km"),
            F.sum("alert").alias("nb_alert")
        ) \
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "nb_plane",
            "average_ground_speed_km_h",
            "average_altitude_geom_km",
            "nb_alert"
        )

    write_batch(
        df_gold_total,
        "airplane_gold_total"
    )

    # GOLD KPI SPEED
    df_gold_kpi_speed = batch_df \
        .groupBy(
            F.window(col("timestamp_ingest"), "10 seconds"),
            "KPI_speed_type"
        ) \
        .agg(
            F.approx_count_distinct("hex").alias("nb_plane_per_speed_type"),
            F.round(F.avg("gs_km_h"), 2).alias("average_ground_speed_km_h_per_speed_type"),
            F.round(F.avg("alt_geom_km"), 2).alias("average_altitude_geom_km_per_speed_type"),
            F.sum("alert").alias("nb_alert_per_speed_type")
        ) \
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "KPI_speed_type",
            "nb_plane_per_speed_type",
            "average_ground_speed_km_h_per_speed_type",
            "average_altitude_geom_km_per_speed_type",
            "nb_alert_per_speed_type"
        )

    write_batch(
        df_gold_kpi_speed,
        "airplane_gold_kpi_speed"
    )

    batch_df.unpersist()