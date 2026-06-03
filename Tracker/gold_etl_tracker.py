# Databricks notebook source
from pyspark.sql.types import *

schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("processing_date", DateType(), True),
    StructField("total_records", IntegerType(), True),
])

gold_etl_tracker = spark.createDataFrame([], schema)

# COMMAND ----------

gold_etl_tracker.write.format("delta").mode("append").saveAsTable("amazon_case_study_gold_tracker")