# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.agg_badge_performance_daily AS
# MAGIC
# MAGIC SELECT
# MAGIC     date,
# MAGIC     is_best_seller,
# MAGIC     is_amazon_choice,
# MAGIC     is_prime,
# MAGIC     climate_pledge,
# MAGIC
# MAGIC     COUNT(*) AS total_products,
# MAGIC
# MAGIC     AVG(rank_by_keyword) AS avg_rank,
# MAGIC     MIN(rank_by_keyword) AS best_rank,
# MAGIC     MAX(rank_by_keyword) AS worst_rank,
# MAGIC
# MAGIC     AVG(page) AS avg_page,
# MAGIC     AVG(page_position) AS avg_page_position,
# MAGIC
# MAGIC     SUM(CASE WHEN page = 1 THEN 1 ELSE 0 END)
# MAGIC         AS page_1_products,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN page = 1 THEN 1 ELSE 0 END)
# MAGIC         / COUNT(*),
# MAGIC         2
# MAGIC     ) AS pct_page_1,
# MAGIC
# MAGIC     SUM(CASE WHEN rank_by_keyword <= 10 THEN 1 ELSE 0 END)
# MAGIC         AS top_10_products,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN rank_by_keyword <= 10 THEN 1 ELSE 0 END)
# MAGIC         / COUNT(*),
# MAGIC         2
# MAGIC     ) AS pct_top_10,
# MAGIC
# MAGIC     AVG(product_price_usd) AS avg_price_usd,
# MAGIC     AVG(discount_pct) AS avg_discount_pct,
# MAGIC
# MAGIC     AVG(product_star_rating) AS avg_rating,
# MAGIC     AVG(product_num_ratings) AS avg_num_ratings,
# MAGIC     AVG(sales_volume_numeric) AS avg_sales_volume,
# MAGIC
# MAGIC     AVG(1.0 / rank_by_keyword) AS visibility_score
# MAGIC
# MAGIC FROM gold.product_visibility_daily
# MAGIC
# MAGIC GROUP BY
# MAGIC     date,
# MAGIC     is_prime,
# MAGIC     is_best_seller,
# MAGIC     is_amazon_choice,
# MAGIC     climate_pledge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.agg_badge_performance_daily

# COMMAND ----------

# agg_badge_df = spark.table("gold.agg_badge_performance_daily")
# export_path = "s3://rabboni-case-study-data/redshift_exports/agg_badge_performance_daily/"

# dbutils.fs.rm(export_path, recurse=True)

# agg_badge_df.write.mode("append")\
#     .format("parquet")\
#         .save(export_path)

# COMMAND ----------

ta = spark.table("gold.agg_badge_performance_daily")
ta.printSchema()

# COMMAND ----------

