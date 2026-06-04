# Databricks notebook source
import pyspark.sql.functions as F
from datetime import date

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Helper_Functions/helper_function

# COMMAND ----------

dim_product = spark.table("gold.dim_product")

dim_search = spark.table("gold.dim_search_term") \
    .select(
        "search_key",
        "search_term"
    )

dim_badge = spark.table("gold.dim_badge_flags") \
    .select(
        "badge_key",
        "is_best_seller",
        "is_amazon_choice",
        "is_prime",
        "climate_pledge"
    )
dim_badge = dim_badge.withColumnRenamed("climate_pledge", "climate_pledge_friendly")

dim_date = spark.table("gold.dim_date") \
    .select(
        "date_key",
        "date"
    )
currency_dim = spark.table("gold.dim_currency_rate")\
    .select(
        "currency_code",
        "usd_conversion_rate"
    )


# COMMAND ----------

latest_timestamp, latest_date = get_latest_gold_timestamp("fact_product_snapshot")

# COMMAND ----------

if latest_timestamp is None:
    fact_product_snapshot_df = spark.read.table('silver.products_table')
else:
    fact_product_snapshot_df = spark.read.table('silver.products_table')\
        .filter((F.col("run_timestamp") > latest_timestamp) & (F.col('date') >= latest_date))
display(fact_product_snapshot_df)

# COMMAND ----------

fact_product_snapshot_df = (
    fact_product_snapshot_df
    .join(
        dim_search,
        on="search_term",
        how="left"
    )
)

fact_product_snapshot_df = (
    fact_product_snapshot_df
    .join(
        dim_badge,
        on=[
            "is_best_seller",
            "is_amazon_choice",
            "is_prime",
            "climate_pledge_friendly"
        ],
        how="left"
    )
)

fact_product_snapshot_df = (
    fact_product_snapshot_df
    .join(
        dim_date,
        on="date",
        how="left"
    )
)

# COMMAND ----------

fact_product_snapshot_df = (
    fact_product_snapshot_df.alias("f")
    .join(
        dim_product.alias("d"),
        (
            (F.col("f.asin") == F.col("d.asin"))
            &
            (F.col("f.run_timestamp") >= F.col("d.valid_from"))
            &
            (
                F.col("d.valid_upto").isNull()
                |
                (
                    F.col("f.run_timestamp")
                    < F.col("d.valid_upto")
                )
            )
        ),
        "left"
    ).select(
        "f.*",
        "d.product_key"
    )
)

# COMMAND ----------

fact_product_snapshot_df = fact_product_snapshot_df.withColumn(
    "discount_pct",
    F.when(
        F.col("product_original_price").isNotNull()
        &
        (F.col("product_original_price") > 0),

        (
            (
                F.col("product_original_price")
                -
                F.col("product_price")
            )
            /
            F.col("product_original_price")
        ) * 100
    )
)

# COMMAND ----------

fact_product_snapshot_df = fact_product_snapshot_df.alias("f")\
    .join(
        currency_dim.alias("c"),
        F.col("f.actual_currency") == F.col("c.currency_code"),
        "left"
    )\
        .withColumn(
        "product_price_usd",
        F.round(
            F.col("product_price") *
            F.col("usd_conversion_rate"),
            2
        )
    )\
        .withColumn(
        "product_original_price_usd",
        F.round(
            F.col("product_original_price") *
            F.col("usd_conversion_rate"),
            2
        )
    )\
        .withColumn(
        "min_offer_price_usd",
        F.round(
            F.col("product_minimum_offer_price") *
            F.col("usd_conversion_rate"),
            2
        )
    )


# COMMAND ----------

fact_product_snapshot_final = (
    fact_product_snapshot_df
    .select(
        F.col("f.product_key").alias("product_key"),

        F.col("f.asin").alias("asin"),

        F.col("search_key"),
        F.col("date_key"),
        F.col("badge_key"),


        F.col("run_timestamp")
            .alias("snapshot_timestamp"),

        F.col("page"),
        F.col("page_position"),
        F.col("rank_by_keyword"),

        F.col("product_price"),
        F.col("product_original_price"),
        F.col("discount_pct"),

        F.col("product_minimum_offer_price")
            .alias("min_offer_price"),

        F.col("actual_currency"),

        F.col("product_star_rating"),

        F.col("product_num_ratings"),
        F.col("product_num_offers"),

        F.col("sales_volume_numeric"),
        F.col("product_price_usd"),
        F.col("product_original_price_usd"),
        F.col("min_offer_price_usd")
    )\
        .withColumn("snapshot_date", F.to_date("snapshot_timestamp"))
)

# COMMAND ----------

fact_product_snapshot_final.display()

# COMMAND ----------

# DBTITLE 1,Cell 14
processed_timestamp = fact_product_snapshot_final.agg(F.max("snapshot_timestamp")).collect()[0][0]
tracker_df = spark.table(GOLD_TRACKER_TABLE)
write_with_tracker(
    df=fact_product_snapshot_final,
    target_table="gold.fact_product_snapshot",
    table_name="fact_product_snapshot",
    tracker_table=GOLD_TRACKER_TABLE,
    tracker_schema=tracker_df.schema,
    processed_timestamp=processed_timestamp
)

# COMMAND ----------

