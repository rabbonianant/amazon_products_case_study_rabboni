# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Helper_Functions/helper_function

# COMMAND ----------

latest_timestamp, latest_date = get_latest_gold_timestamp("product_visibility_daily")

# COMMAND ----------

if latest_timestamp:
    fact_df = spark.table("gold.fact_product_snapshot").filter((F.col("snapshot_timestamp") > latest_timestamp) & (F.col('snapshot_date') >= latest_date))
else:
    fact_df = spark.table("gold.fact_product_snapshot")

# COMMAND ----------

product_df = spark.table("gold.dim_product")
search_df = spark.table("gold.dim_search_term")
date_df = spark.table("gold.dim_date")
badge_df = spark.table("gold.dim_badge_flags")

# COMMAND ----------

reporting_df = (
    fact_df.alias("f")
    .join(product_df.alias("p"), "product_key")
    .join(search_df.alias("s"), "search_key")
    .join(date_df.alias("d"), "date_key")
    .join(badge_df.alias("b"), "badge_key")
    .select(
        "f.date_key",
        "d.date",

        "f.search_key",
        "s.search_term",

        "f.product_key",
        "f.asin",

        "p.product_title",

        "f.actual_currency",

        "f.page",
        "f.page_position",
        "f.rank_by_keyword",

        "f.product_price",
        "f.product_price_usd",

        "f.product_original_price",
        "f.product_original_price_usd",

        "f.discount_pct",

        "f.product_star_rating",
        "f.product_num_ratings",
        "f.sales_volume_numeric",

        "b.badge_combination",

        "b.is_best_seller",
        "b.is_amazon_choice",
        "b.is_prime",
        "b.climate_pledge",

        "d.year",
        "d.quarter",
        "d.month",
        "d.month_name",
        "d.week",
        "d.week_of_year",
        "d.day",
        "d.day_of_week_num",
        "d.is_weekend",
        "d.is_month_start",
        "d.is_month_end",
        "d.is_quarter_start",
        "d.is_quarter_end",

        "f.snapshot_timestamp"
    )
)

# COMMAND ----------

export_path = "s3://rabboni-case-study-data/redshift_exports/product_visibility_daily/temp/"
processed_timestamp = reporting_df.agg(F.max("snapshot_timestamp")).collect()[0][0]
tracker_table = spark.table(GOLD_TRACKER_TABLE)
tracker_schema = tracker_table.schema

records_written = reporting_df.count()
try:
    reporting_df.write.mode("append").parquet(export_path)
    tracker_record = spark.createDataFrame(
        [
            (
                "product_visibility_daily",
                "gold",
                "success",
                processed_timestamp,
                processed_timestamp.date(),
                records_written
                )
            ],
            schema=tracker_schema
        )
    
    tracker_record.write\
        .format("delta")\
            .mode("append")\
                .saveAsTable(GOLD_TRACKER_TABLE)
     
except Exception as e:
        tracker_record = spark.createDataFrame(
            [
                (
                    "product_visibility_daily",
                    "gold",
                    "failed",
                    processed_timestamp,
                    processed_timestamp.date(),
                    0
                )
            ],
            schema=tracker_schema
        )
        tracker_record.write\
            .format("delta")\
                .mode("append")\
                    .saveAsTable(GOLD_TRACKER_TABLE)

        raise e


# COMMAND ----------

temp_path = "s3://rabboni-case-study-data/redshift_exports/product_visibility_daily/temp/"
export_path = "s3://rabboni-case-study-data/redshift_exports/product_visibility_daily/export/"

files = dbutils.fs.ls(temp_path)

for file in files:
    if file.path.endswith(".parquet"):
        file_name = file.name

        source = file.path
        destination = export_path + file_name
        dbutils.fs.cp(source, destination)

        print(f"Copied: {file_name}")