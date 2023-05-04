-- Databricks notebook source
create table if not exists books_silver_TGT
(
book_id STRING,title STRING,author STRING,price DOUBLE,current BOOLEAN,effective_date TIMESTAMP,end_date TIMESTAMP
)

-- COMMAND ----------

create table if not exists books_silver_SRC
(
book_id String,title String,author String,price double,updated Timestamp
)

-- COMMAND ----------

insert into books_silver_SRC values(1,'a','a1',3,current_timestamp())

-- COMMAND ----------

select 
select a.*,row_number() over (partition by book_id order by updated desc) as rn from books_silver_SRC a


-- COMMAND ----------

update books_silver_SRC 
set price=2

-- COMMAND ----------

select * from books_silver_SRC

-- COMMAND ----------

--correct this query mulitple inserts are not working databricks merge
Merge into books_silver_TGT TGT
using (
select s.book_id as merge_key ,s.* from books_silver_SRC s
union
select null as merge_key,s.* from books_silver_SRC s inner join books_silver_TGT t on
s.book_id=t.book_id and t.current=true and s.price!=t.price

) src
on
TGT.book_id=src.merge_key 
when MATCHED and TGT.current=true and SRC.price!=TGT.price then
update set current=False,end_date=src.updated

when not matched then
insert (book_id ,title ,author ,price ,current ,effective_date ,end_date ) values(src.book_id,src.title,src.author,src.price,true,src.updated,null)

-- COMMAND ----------

select * from books_silver_TGT

-- COMMAND ----------

select * from books_silver_src