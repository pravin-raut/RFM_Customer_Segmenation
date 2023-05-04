# Databricks notebook source
# MAGIC %run /Users/praut1606@gmail.com/RFMAnalysis/00.Initialization

# COMMAND ----------

dataset_bookstore="dbfs:/FileStore/tables/RFM"

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}")
display(files)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
    
    schema="key binary, value binary,topic string,partition long,offsert long,timestamp long"
    
    query=(spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","json").schema(schema).load(f"{dataset_bookstore}")\
        .withColumn("timestamp",(F.col("timestamp")/1000).cast("timestamp"))\
        .withColumn("year_month",F.date_format("timestamp","yyyy-MM"))\
        .writeStream\
        .option("checkpointLocation","dbfs:/mnt/demo-pro/checkpoints/bronze")\
        .option("mergeSchema",True)\
        .partitionBy("topic","year_month")\
        .trigger(availableNow=True)\
        .table("bronze_multiplex"))
    
    query.awaitTermination()
        
    
       
        

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC select topic,count(*) from bronze_multiplex
# MAGIC group by topic

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze_multiplex