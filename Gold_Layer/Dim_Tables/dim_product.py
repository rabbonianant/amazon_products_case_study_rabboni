# Databricks notebook source
import pyspark.sql.functions as F
from datetime import date
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Helper_Functions/helper_function

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

dim_product_df_with_scd_columns = dim_product_df_with_scd_columns.withColumn(
    "hash_value", F.md5(
        F.concat_ws(
            "||",
            F.coalesce(F.col("product_title"), F.lit("")),
            F.coalesce(F.col("product_url"), F.lit("")),
            F.coalesce(F.col("product_photo"), F.lit("")),
            F.coalesce(F.col("has_variations").cast("string"), F.lit(""))
        )
    )
)

# COMMAND ----------


def dim_product_scd2(silver_incremental_df,dim_table_name="gold.dim_product"):
    current_dim = spark.table(dim_table_name).filter(F.col("is_current") == True)
    
    w = Window.partitionBy("asin").orderBy(F.desc("run_timestamp"))
    incoming = silver_incremental_df.withColumn("rn", F.row_number().over(w))\
        .filter(F.col("rn") == 1)\
            .drop("rn")

    comparison = incoming.alias("s").join(current_dim.alias("d"), on="asin", how="left")
    new_products = comparison.filter(F.col("d.asin").isNull())

    changed_products = comparison.filter(
        (F.col("d.asin").isNotNull()) &
        (F.col("s.hash_value") != F.col("d.hash_value"))
    )

    changed_expiry = changed_products.select(
            F.col("asin"),
            F.col("s.run_timestamp").alias("new_valid_upto")
        )

    if not changed_expiry.isEmpty():
        delta_table = DeltaTable.forName(spark,dim_table_name)
        delta_table.alias("t").merge(
            changed_expiry.alias("s"),
            """
            t.asin = s.asin
            AND t.is_current = true
            """
        ).whenMatchedUpdate(
            set={
                "is_current": "false",
                "valid_upto": "s.new_valid_upto"
            }
        ).execute()

    changed_versions = changed_products.select(
            F.col("s.asin"),
            F.col("s.product_title"),
            F.col("s.product_url"),
            F.col("s.product_photo"),
            F.col("s.has_variations"),
            F.col("s.hash_value"),
            F.col("s.run_timestamp")
        )\
            .withColumn("valid_from",F.col("run_timestamp"))\
                .withColumn("valid_upto",F.lit(None).cast("timestamp"))\
                    .withColumn("is_current",F.lit(True))\
                        .drop("run_timestamp")

    new_versions = new_products.select(
            F.col("s.asin"),
            F.col("s.product_title"),
            F.col("s.product_url"),
            F.col("s.product_photo"),
            F.col("s.has_variations"),
            F.col("s.hash_value"),
            F.col("s.run_timestamp")
        )\
            .withColumn("valid_from",F.col("run_timestamp"))\
                .withColumn("valid_upto",F.lit(None).cast("timestamp"))\
                    .withColumn("is_current",F.lit(True))\
                        .drop("run_timestamp")

    inserts = changed_versions.unionByName(new_versions)

    return inserts

# COMMAND ----------

processed_timestamp = dim_product_df_with_scd_columns.agg(F.max("run_timestamp")).collect()[0][0]
tracker_schema = tracker_df.schema

if spark.table(GOLD["DIM_PRODUCT"]).isEmpty():
    write_df = dim_product_df_with_scd_columns.drop("run_timestamp")
else:
    write_df = dim_product_scd2(dim_product_df_with_scd_columns)

write_with_tracker(
    df=write_df,
    target=GOLD["DIM_PRODUCT"],
    table_name="dim_product",
    tracker_table=GOLD_TRACKER_TABLE,
    tracker_schema=tracker_schema,
    processed_timestamp=processed_timestamp,
    write_type="delta"
)