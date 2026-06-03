# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("source", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("ingestion_date", DateType(), True),
    StructField("total_records", IntegerType(), True),
])

amazon_case_study_tracker = spark.createDataFrame([],schema)


# COMMAND ----------

amazon_case_study_tracker.write.format("delta").mode("append").saveAsTable("amazon_case_study_silver_tracker")
