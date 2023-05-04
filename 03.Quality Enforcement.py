# Databricks notebook source
# MAGIC %run "/Users/praut1606@gmail.com/Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets"

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table orders_Silver add constraint timestamp_within_range check(order_timestamp>='2020-01-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended orders_Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC --alter table orders_silver add constraint valid_quantity check(quantity>0);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver where quantity<=0

# COMMAND ----------

spark.read.table("bronze_multiplex").filter("topic='orders'").count()