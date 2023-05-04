# Databricks notebook source
checkpoint_path = "dbfs:/mnt/demo_pro/checkpoints/"
dataset_bookstore="dbfs:/FileStore/tables/RFM"

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/RFM")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists RFM_Analysis;
# MAGIC use RFM_Analysis;

# COMMAND ----------

def cleanup():
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/bronze",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/books_silver",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/books_silver1",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/orders_silver",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/orders_silver1",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/cusomer_silver",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/cusomers_order",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/book_sales",True)
    dbutils.fs.rm("dbfs:/mnt/demo-pro/checkpoints/authour_Stats",True)

    spark.sql("drop database RFM_Analysis cascade")


# COMMAND ----------

#cleanup()
#dbutils.fs.rm("dbfs:/FileStore/tables/RFM/",True)
#dbutils.fs.mkdirs("dbfs:/FileStore/tables/RFM/")


# COMMAND ----------

#dbutils.fs.cp('dbfs:/mnt/demo-datasets/DE-Pro/bookstore/books-updates-streaming/','dbfs:/FileStore/tables/RFM/',True)

# COMMAND ----------

