# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA silver
# MAGIC MANAGED LOCATION 's3://rabboni-case-study-data/bronze/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA gold
# MAGIC MANAGED LOCATION 's3://rabboni-case-study-data/bronze/'

# COMMAND ----------

# MAGIC %sql 
# MAGIC Create table if not exists silver.products_table
# MAGIC using delta
# MAGIC LOCATION 's3://rabboni-case-study-data/silver/products_table'