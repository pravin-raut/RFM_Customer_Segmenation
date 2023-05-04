# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Users/praut1606@gmail.com/RFMAnalysis/00.Initialization

# COMMAND ----------

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"
customer_df=spark.read.table("bronze_multiplex")\
  .filter("topic='customers'")\
  .select(F.from_json(F.col("value").cast("string"),schema).alias("v"))\
  .select("v.*")\
  .filter(F.col("row_status").isin(['insert','update']))

# COMMAND ----------

from pyspark.sql.window import Window

window=Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
#ranked_df=(customer_df.withColumn("rank",F.rank().over(window))).filter(F.col("rank")==1).drop(F.col("rank"))
ranked_df=(customer_df.withColumn("rank",F.rank().over(window))).filter(F.col("rank")==1).drop("rank")
ranked_df.display()

# COMMAND ----------

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"
ranked_df=spark.read.table("bronze_multiplex")\
  .filter("topic='customers'")\
  .select(F.from_json(F.col("value").cast("string"),schema).alias("v"))\
  .select("v.*")\
  .filter(F.col("row_status").isin(['insert','update']))\
  .withColumn("rank",F.rank().over(window)).filter(F.col("rank")==1).drop("rank")
  

ranked_df.display()


# COMMAND ----------

def batch_upsert(microBatchDF,bathcId):
  window=Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
  microBatchDF.filter(F.col("row_status").isin(['insert','update']))\
    .withColumn("rank",F.rank().over(window))\
    .filter(F.col("rank")==1)\
    .drop("rank")\
    .createOrReplaceTempView("ranked_updates")

  Query=""" 

    merge into customers_silver c
    using ranked_updates r
    on
    c.customer_id=r.customer_id
    when matched and c.row_time<r.row_time
    then update set *
    when not matched
    then insert *

    """
  microBatchDF.sparkSession.sql(Query)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

df_country_lookup=spark.read.json("dbfs:/mnt/demo-datasets/DE-Pro/bookstore/country_lookup/country_lookup.json")
df_country_lookup.display()

# COMMAND ----------

query =(spark.readStream.table("bronze_multiplex")\
  .filter("topic='customers'")\
  .select(F.from_json(F.col("value").cast("string"),schema).alias("v"))\
  .select("v.*")\
  .join(F.broadcast(df_country_lookup),F.col("country_code")==F.col("code"),"inner")\
  .writeStream\
  .foreachBatch(batch_upsert)\
  .option("checkpointLocation","dbfs:/mnt/demo-pro/checkpoints/cusomer_silver")\
  .trigger(availableNow=True)
  .start()
)

query.awaitTermination()

  

# COMMAND ----------

count=spark.read.table("customers_silver").count()
expected_Count=spark.read.table("customers_silver").select("customer_id").distinct().count()

assert count==expected_Count
print("unit test passed")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC alter table customers_silver
# MAGIC set tblproperties(delta.enableChangeDataFeed=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("customers_silver",2)

# COMMAND ----------

cdf_df=spark.readStream.format("delta").option("readChangeData",True).option("startingVersion",2).table("customers_silver")
cdf_df.display()

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/rfm_analysis.db/customers_silver"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/rfm_analysis.db/customers_silver/_change_data/"))

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

# COMMAND ----------

