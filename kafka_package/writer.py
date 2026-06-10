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