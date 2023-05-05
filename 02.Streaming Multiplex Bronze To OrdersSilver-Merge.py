# Databricks notebook source
# MAGIC %run ./01.SetUp

# COMMAND ----------

# MAGIC %md
# MAGIC For example, suppose the current watermark is "2023-04-06 10:00:00", and an event with a timestamp of "2023-04-06 09:55:00" arrives. Since the event time is within the 10-minute threshold of the watermark, it will be processed normally. However, if an event with a timestamp of "2023-04-06 09:50:00" arrives, it will be dropped because its event time is earlier than the threshold.
# MAGIC
# MAGIC In Spark Streaming, the watermark is dynamically updated based on the timestamps of the incoming data. When a new batch of data arrives, the system updates the watermark to the maximum event time seen so far minus the watermark threshold.
# MAGIC
# MAGIC Let's say you have a stream with 10 records, each with a different timestamp value. The system will process each record as it arrives, and update the watermark accordingly. If the maximum event time seen so far is "2023-04-06 10:00:00", and the watermark threshold is 10 minutes, then the current watermark will be "2023-04-06 09:50:00".
# MAGIC
# MAGIC Note that the watermark is not based on a fixed interval, but rather on the maximum event time seen so far. Therefore, the actual watermark value will depend on the distribution of event times in the stream. If most of the events have timestamps close to each other, then the watermark will be close to the maximum event time. If the events are spread out over a longer period of time, then the watermark will be further behind the maximum event time.

# COMMAND ----------

import pyspark.sql.functions as F

json_schema="order_id String,order_timestamp Timestamp,customer_id string,quantity BIGINT,total BIGINT,books ARRAY<STRUCT<book_id STRING,quantity BIGINT,subtotal BIgint>>"
deduped_orders_df=(
    spark.readStream.table("bronze_multiplex")\
        .filter("topic='orders'")\
        .select(F.from_json(F.col("value").cast("string"),json_schema).alias("v"))\
        .select("v.*")\
        .withWatermark("order_timestamp","30 seconds")\
        .filter("quantity>=0")\
        .dropDuplicates(["order_id","order_timestamp"])\

)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists orders_silver
# MAGIC (order_id String,order_timestamp Timestamp,customer_id string,quantity BIGINT,total BIGINT,books ARRAY<STRUCT<book_id STRING,quantity BIGINT,subtotal BIgint>>)

# COMMAND ----------

# MAGIC %sql
# MAGIC --alter table orders_Silver add constraint timestamp_within_range check(order_timestamp>='2020-01-01')

# COMMAND ----------

def upsert_data(microBatchDF,batch):
    microBatchDF.createOrReplaceTempView("Orders_microbatch")
    
    sql_query="""
    Merge into orders_silver a
    using Orders_microbatch b
    on
    a.order_id=b.order_id
    and
    a.order_timestamp=b.order_timestamp
    when not matched then insert *
    
    """
    
    #spark.sql(sql_query)
    #here spark session cannot be access from microbatch , instead we need to access local microbatch dataframe
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

query =(deduped_orders_df.writeStream
                .foreachBatch(upsert_data)
                .option("checkpointLocation",CheckpointLocation+"/orders_silver1")
                .trigger(availableNow=True)
                .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from orders_silver
