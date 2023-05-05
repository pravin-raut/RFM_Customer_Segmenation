# Databricks notebook source
# MAGIC %run ./01.SetUp

# COMMAND ----------

from pyspark.sql import functions as F

def process_book_sales():

  orders_df=(spark.readStream.table("orders_silver")
                  .withColumn("book",F.explode("books"))
  )

  book_df=spark.read.table("current_books")

  query=(
orders_df.join(book_df,orders_df.book.book_id==orders_df.book.book_id,"inner")
          .writeStream
            .outputMode("append")
            .option("checkpointLocation","dbfs:/mnt/demo-pro/checkpoints/book_sales")
            .trigger(availableNow=True)
            .table("book_sales")
  )

  query.awaitTermination()


# COMMAND ----------

spark.read.table("orders_silver").display()

# COMMAND ----------

process_book_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from book_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC   FROM customers_orders
# MAGIC   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries_stats_vw where country="France"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from book_sales

# COMMAND ----------

from pyspark.sql import functions as F
query=(
spark.readStream
      .table("book_sales")
      .withWatermark("order_timestamp","10 Minutes")
      .groupBy(
        F.window("order_timestamp","5 minutes").alias("time"),"author"

      )
      .agg(
        F.count("order_id").alias("order_quantity"),
        F.avg("quantity").alias("avg_quantity")
      )
      .writeStream
      .option("checkpointLocation","dbfs:/mnt/demo-pro/checkpoints/authour_Stats")
      .trigger(availableNow=True)
      .table("author_stats")

)
query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_stats
