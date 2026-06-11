# Databricks notebook source
import pyspark.sql.functions as F
from datetime import date
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Utility_Functions/util_functions

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

latest_timestamp, latest_date = get_latest_gold_timestamp("dim_product")

# COMMAND ----------

dim_product_columns = GOLD['DIM_PRODUCT_COLUMNS']
tracker_df = spark.table(GOLD_TRACKER_TABLE)

# COMMAND ----------

if latest_timestamp is None:
    dim_product_df = spark.read.table(SILVER["PRODUCTS_TABLE"])\
        .select(dim_product_columns)
else:
    dim_product_df = spark.read.table(SILVER["PRODUCTS_TABLE"])\
        .filter((F.col('run_timestamp') > latest_timestamp) & (F.col('date') >= latest_date))\
            .select(dim_product_columns)


# COMMAND ----------

dim_product_df_with_scd_columns = dim_product_df.withColumn("valid_from", F.col('run_timestamp'))\
    .withColumn("valid_upto", F.lit(None).cast("timestamp"))\
        .withColumn("is_current", F.lit(True))

# COMMAND ----------

hash_columns = GOLD["DIM_PRODUCT_HASH_COLUMNS"]

dim_product_df_with_hash_value = dim_product_df_with_scd_columns.withColumn(
    "hash_value",
    F.md5(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(column).cast("string"),
                    F.lit("")
                )
                for column in hash_columns
            ]
        )
    )
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def dim_product_scd2(incoming_df):
    change_window = Window.partitionBy("asin").orderBy("run_timestamp")

    incoming_df = incoming_df.withColumn("previous_hash",F.lag("hash_value").over(change_window))\
        .filter(F.col("previous_hash").isNull() | (F.col("hash_value") != F.col("previous_hash")))\
            .drop("previous_hash")

    current_dim = spark.table(GOLD["DIM_PRODUCT"])\
        .filter(F.col("is_current"))\
            .join(incoming_df.select("asin").distinct(), "asin", "inner")

    comparison_df = incoming_df.alias("s").join(current_dim.alias("d"),"asin","left")

    delta_dim = DeltaTable.forName(spark,GOLD["DIM_PRODUCT"])
    delta_dim.alias("d").merge(
        (comparison_df.filter(F.col("d.hash_value").isNotNull()
                              &
                              (F.col("s.hash_value") != F.col("d.hash_value"))
                )\
                    .select("asin",F.col("s.run_timestamp").alias("new_valid_from"))\
                        .distinct()
            ).alias("s"),
            """
            d.asin = s.asin
            AND d.is_current = true
            """
        )\
            .whenMatchedUpdate(
                set={
                    "valid_upto": "s.new_valid_from",
                    "is_current": "false"
                }
            )\
                .execute()

    scd_window = (Window.partitionBy("asin").orderBy("run_timestamp"))

    insert_df = comparison_df.filter(F.col("d.hash_value").isNull() | (F.col("s.hash_value") != F.col("d.hash_value")))\
        .select(*[F.col(f"s.{c}")for c in incoming_df.columns])\
            .withColumn("valid_from", F.col("run_timestamp"))\
                .withColumn("valid_upto",F.lead("run_timestamp").over(scd_window))\
                    .withColumn("is_current",F.when(F.col("valid_upto").isNull(),True).otherwise(False))\
                        .drop(F.col("run_timestamp"))
    
    return insert_df

# COMMAND ----------

processed_timestamp = dim_product_df_with_scd_columns.agg(F.max("run_timestamp")).collect()[0][0]
tracker_schema = tracker_df.schema

if spark.table(GOLD["DIM_PRODUCT"]).isEmpty():
    write_df = dim_product_df_with_hash_value.drop("run_timestamp")
else:
    write_df = dim_product_scd2(dim_product_df_with_hash_value)

write_with_tracker(
    df=write_df,
    target=GOLD["DIM_PRODUCT"],
    table_name="dim_product",
    tracker_table=GOLD_TRACKER_TABLE,
    tracker_schema=tracker_schema,
    processed_timestamp=processed_timestamp,
    write_type="delta"
)