-- Databricks notebook source
-- MAGIC %run "./Cleanup Database and Datasets" $bronze_base_dir=/public/retail_db_bronze $gold_base_dir=/public/retail_db_gold

-- COMMAND ----------

-- MAGIC %run "./File Format Converter" $ds=orders $src_base_dir=/public/retail_db $bronze_base_dir=/public/retail_db_bronze

-- COMMAND ----------

-- MAGIC %run "./File Format Converter" $ds=order_items $src_base_dir=/public/retail_db $bronze_base_dir=/public/retail_db_bronze

-- COMMAND ----------

-- MAGIC %run "./Create Spark SQL Tables" $table_name=orders $bronze_base_dir=/public/retail_db_bronze

-- COMMAND ----------

-- MAGIC %run "./Create Spark SQL Tables" $table_name=order_items $bronze_base_dir=/public/retail_db_bronze

-- COMMAND ----------

SHOW tables

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

SELECT count(*) FROM order_items

-- COMMAND ----------

-- MAGIC %run "./Daily Product Revenue" $gold_base_dir=/public/retail_db_gold

-- COMMAND ----------


