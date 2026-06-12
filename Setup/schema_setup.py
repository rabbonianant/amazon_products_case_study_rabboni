# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA silver
# MAGIC MANAGED LOCATION 's3://rabboni-case-study-data/bronze/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA gold
# MAGIC MANAGED LOCATION 's3://rabboni-case-study-data/bronze/'