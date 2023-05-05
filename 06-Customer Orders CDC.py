# Databricks notebook source
# MAGIC %run ./01.SetUp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --alter table customers_silver set tblproperties(delta.enableChangeDataFeed=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("customers_silver",2)

# COMMAND ----------

cdf_df=spark.read.format("delta").option("readChangeData",True).option("startingVersion",2).table("customers_silver")
cdf_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_orders
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def batch_upsert(microBatchDF,batchID):
  window=Window.partitionBy("order_id","customer_id").orderBy(F.col("_commit_timestamp").desc())
  microBatchDF.filter(F.col("_change_type").isin(['insert','update_postimage']))\
    .withColumn("rank",F.rank().over(window))\
    .filter("rank=1")\
    .drop("rank","_change_type","_commit_version")\
    .withColumnRenamed("_commit_timestamp","processed_timestamp")\
    .createOrReplaceTempView("ranked_updates")

  Query="""
      Merge into customers_orders c
      using ranked_updates r
      on
      c.order_id=r.order_id and c.customer_id=r.customer_id
      when matched and c.processed_timestamp<r.processed_timestamp
      then update set *
      when not matched then insert *

"""

  microBatchDF.sparkSession.sql(Query)


# COMMAND ----------

def process_customer_orders():
  orders_df=spark.readStream.table("orders_silver")

  cdf_customer_df=(spark.readStream
                        .option("readChangeData",True)
                        .option("startingVersion",2)
                        .table("customers_silver")
                  )
  query=(orders_df
                  .join(cdf_customer_df,["customer_id"],"inner")
                  .writeStream
                    .foreachBatch(batch_upsert)
                    .option("checkpointLocation","dbfs:/mnt/demo-pro/checkpoints/cusomers_order")
                    .trigger(availableNow=True)
                    .start()
  
  )
  query.awaitTermination()

  

# COMMAND ----------

process_customer_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_orders
