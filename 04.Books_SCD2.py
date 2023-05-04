# Databricks notebook source
# MAGIC %run /Users/praut1606@gmail.com/RFMAnalysis/00.Initialization

# COMMAND ----------

json_schema="book_id String,title String,author String,price double,updated Timestamp"

import pyspark.sql.functions as F
spark.read.table("bronze_multiplex")\
    .select(F.from_json(F.col("value").cast("string"),json_schema).alias("v"))\
    .select("v.*")\
    .filter("topic='books'")\
    .display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists books_silver
# MAGIC (
# MAGIC book_id STRING,title STRING,author STRING,price DOUBLE,current BOOLEAN,effective_date TIMESTAMP,end_date TIMESTAMP
# MAGIC )

# COMMAND ----------

def type2_upsert(microBatchDF,batch):
    microBatchDF.createOrReplaceTempView("books_silver_SRC")
    
    sql_query="""
Merge into books_silver TGT
using (
select s.book_id as merge_key ,s.* from books_silver_SRC s
union
select null as merge_key,s.* from books_silver_SRC s inner join books_silver t on
s.book_id=t.book_id and t.current=true and s.price!=t.price

) src
on
TGT.book_id=src.merge_key 
when MATCHED and TGT.current=true and SRC.price!=TGT.price  then
update set current=False,end_date=src.updated

when not matched then
insert (book_id ,title ,author ,price ,current ,effective_date ,end_date ) values(src.book_id,src.title,src.author,src.price,true,src.updated,null)
      
    
    """
    
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

def process_books():
    json_schema="book_id String,title String,author String,price double,updated Timestamp"

    import pyspark.sql.functions as F
    query=( spark.readStream.table("bronze_multiplex")\
        .select(F.from_json(F.col("value").cast("string"),json_schema).alias("v"))\
        .select("v.*")\
        .filter("topic='books'")\
        .dropDuplicates(["book_id","updated"])\
        .writeStream
        .foreachBatch(type2_upsert)
        .option("checkpointLocation","dbfs:/mnt/demo-pro/checkpoints/books_silver1")
        .trigger(availableNow=True)
        .start()
)

    query.awaitTermination()


# COMMAND ----------

process_books()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_silver where Book_id='B13' order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table current_books
# MAGIC AS
# MAGIC select book_id,title,author,price from books_silver where CURRENT=true

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from current_books