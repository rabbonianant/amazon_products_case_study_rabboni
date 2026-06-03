# Databricks notebook source
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Helper_Functions/helper_function

# COMMAND ----------

tracker_df = spark.table(GOLD_TRACKER_TABLE)

# COMMAND ----------

latest_timestamp, latest_date = get_latest_gold_timestamp("dim_search_term")

if latest_timestamp is None:
    silver_incremental = spark.read.table('silver.products_table')
else:
    silver_incremental = spark.read.table("silver.products_table")\
        .filter((F.col("run_timestamp") > latest_timestamp) & (F.col('date') >= latest_date))


# COMMAND ----------

def build_dim_search_term(silver_incremental_df):
    dim_search_df = spark.table("gold.dim_search_term")
    search_terms_df = silver_incremental_df.select("search_term")\
        .distinct()\
            .withColumn(
            "keyword_count",
            F.size(
                F.split(
                    F.trim(F.col("search_term")),
                    r"\s+"
                )
            )
        )

    new_search_terms = search_terms_df.alias("new").join(
            dim_search_df.alias("existing"),
            on="search_term",
            how="left_anti"
        )

    return new_search_terms

# COMMAND ----------

new_terms = build_dim_search_term(silver_incremental)

processed_timestamp = silver_incremental.agg(F.max("run_timestamp")).collect()[0][0]

write_with_tracker(
    df=new_terms,
    target_table="gold.dim_search_term",
    table_name="dim_search_term",
    tracker_table=GOLD_TRACKER_TABLE,
    tracker_schema=tracker_df.schema,
    processed_timestamp=processed_timestamp
)

# COMMAND ----------

