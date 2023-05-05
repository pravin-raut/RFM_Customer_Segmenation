-- Databricks notebook source
USE RFM_Analysis;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create or replace temp view Books_customer_detail
-- MAGIC as
-- MAGIC with Books_Detail
-- MAGIC as
-- MAGIC (
-- MAGIC select order_id,order_timestamp,customer_id,quantity,total,explode(books) as book
-- MAGIC from orders_silver
-- MAGIC )
-- MAGIC select order_id,order_timestamp,customer_id,quantity,total,book.book_id,book.quantity as quantity_book,book.subtotal
-- MAGIC from Books_Detail

-- COMMAND ----------

Create or replace table Cusomter_Orders_Books
as
select c.customer_id,cast(B.order_timestamp as date) as order_date,bk.price*B.quantity as Total_Amoumt from Customers_silver C
inner join Books_customer_detail B
ON C.customer_id=B.customer_id  
inner join books_silver bk
ON bk.book_id=B.book_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_date_value = spark.sql("select cast(max(order_date)+ interval 1 day as date) as max_date from Cusomter_Orders_Books").collect()[0]['max_date']
-- MAGIC # Convert max_date_value to a string in the YYYY-MM-DD format
-- MAGIC max_date_str = max_date_value.strftime('%Y-%m-%d')
-- MAGIC max_date_str
-- MAGIC # Compute the Recency_Score as a date value

-- COMMAND ----------

Create or replace Temporary view RFM_Segmentation_Temp
as
select customer_id,max(order_date) as Recency,count(distinct order_date) as Frequencey, sum(Total_Amoumt) as Monetory
from Cusomter_Orders_Books
group by customer_id

-- COMMAND ----------

describe table extended  RFM_Segmentation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_date_value = spark.sql("select max(Recency) + interval 1 day as max_date from RFM_Segmentation_Temp").collect()[0]['max_date'].strftime('%Y-%m-%d')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""Create or replace Table RFM_Segmentation
-- MAGIC select customer_id, datediff('{}',Recency) as RecenyDays,Frequencey,Monetory from RFM_Segmentation_Temp""".format(max_date_value))

-- COMMAND ----------

select * from RFM_Segmentation
