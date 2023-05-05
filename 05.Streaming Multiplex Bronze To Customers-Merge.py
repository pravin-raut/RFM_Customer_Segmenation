# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %run ./01.SetUp

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

df_country_lookup=spark.read.json(CountryLookup+"/country_lookup.json")
df_country_lookup.display()

# COMMAND ----------

query =(spark.readStream.table("bronze_multiplex")\
  .filter("topic='customers'")\
  .select(F.from_json(F.col("value").cast("string"),schema).alias("v"))\
  .select("v.*")\
  .join(F.broadcast(df_country_lookup),F.col("country_code")==F.col("code"),"inner")\
  .writeStream\
  .foreachBatch(batch_upsert)\
  .option("checkpointLocation",CheckpointLocation+"cusomer_silver")\
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



# COMMAND ----------


