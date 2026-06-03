# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.agg_keyword_visibility_daily AS
# MAGIC
# MAGIC SELECT
# MAGIC     date,
# MAGIC     search_term,
# MAGIC
# MAGIC     COUNT(*) AS total_products,
# MAGIC
# MAGIC     AVG(product_price_usd) AS avg_price_usd,
# MAGIC     MIN(product_price_usd) AS min_price_usd,
# MAGIC     MAX(product_price_usd) AS max_price_usd,
# MAGIC
# MAGIC     AVG(product_original_price_usd) AS avg_original_price_usd,
# MAGIC
# MAGIC     AVG(discount_pct) AS avg_discount_pct,
# MAGIC
# MAGIC     AVG(product_star_rating) AS avg_rating,
# MAGIC
# MAGIC     AVG(product_num_ratings) AS avg_num_ratings,
# MAGIC
# MAGIC     AVG(sales_volume_numeric) AS avg_sales_volume,
# MAGIC
# MAGIC     SUM(CASE WHEN is_best_seller THEN 1 ELSE 0 END)
# MAGIC         AS best_seller_products,
# MAGIC
# MAGIC     SUM(CASE WHEN is_amazon_choice THEN 1 ELSE 0 END)
# MAGIC         AS amazon_choice_products,
# MAGIC
# MAGIC     SUM(CASE WHEN is_prime THEN 1 ELSE 0 END)
# MAGIC         AS prime_products,
# MAGIC
# MAGIC     SUM(CASE WHEN climate_pledge THEN 1 ELSE 0 END)
# MAGIC         AS climate_pledge_products,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN is_best_seller THEN 1 ELSE 0 END)
# MAGIC         / COUNT(*),
# MAGIC         2
# MAGIC     ) AS pct_best_seller,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN is_amazon_choice THEN 1 ELSE 0 END)
# MAGIC         / COUNT(*),
# MAGIC         2
# MAGIC     ) AS pct_amazon_choice,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN is_prime THEN 1 ELSE 0 END)
# MAGIC         / COUNT(*),
# MAGIC         2
# MAGIC     ) AS pct_prime,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN climate_pledge THEN 1 ELSE 0 END)
# MAGIC         / COUNT(*),
# MAGIC         2
# MAGIC     ) AS pct_climate_pledge,
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
# MAGIC     ) AS pct_top_10
# MAGIC
# MAGIC FROM gold.product_visibility_daily
# MAGIC
# MAGIC GROUP BY
# MAGIC     date,
# MAGIC     search_term;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.agg_keyword_visibility_daily

# COMMAND ----------

tab = spark.table('gold.agg_keyword_visibility_daily')
tab.printSchema()

# COMMAND ----------

