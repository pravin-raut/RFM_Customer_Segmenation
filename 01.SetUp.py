# Databricks notebook source
BasePath="/mnt/rfm/"
SourceFiles=BasePath+"Source/"
Customer=SourceFiles+"Customer"
CountryLookup=SourceFiles+"country_lookup"
TargetLocation=BasePath+"Target/"
SchemaLocation=BasePath+"Schema/"
CheckpointLocation=BasePath+"CheckPoint/"
StorageLocation=BasePath+"Storage/"



# COMMAND ----------

dbutils.fs.mkdirs(SourceFiles)
dbutils.fs.mkdirs(Customer)
dbutils.fs.mkdirs(CountryLookup)
dbutils.fs.mkdirs(TargetLocation)
dbutils.fs.mkdirs(SchemaLocation)
dbutils.fs.mkdirs(CheckpointLocation)
dbutils.fs.mkdirs(StorageLocation)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists RFM_Analysis;
# MAGIC use RFM_Analysis;
