# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_product_snapshot (
# MAGIC     snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     product_key BIGINT,
# MAGIC     asin STRING,
# MAGIC
# MAGIC     search_key BIGINT,
# MAGIC     date_key BIGINT,
# MAGIC     badge_key BIGINT,
# MAGIC     currency_key BIGINT,
# MAGIC
# MAGIC     snapshot_timestamp TIMESTAMP,
# MAGIC     snapshot_date DATE,
# MAGIC
# MAGIC     page INT,
# MAGIC     page_position INT,
# MAGIC     rank_by_keyword INT,
# MAGIC
# MAGIC     product_price DOUBLE,
# MAGIC     product_original_price DOUBLE,
# MAGIC     discount_pct DOUBLE,
# MAGIC     product_price_usd DOUBLE,
# MAGIC     product_original_price_usd DOUBLE,
# MAGIC
# MAGIC
# MAGIC     min_offer_price DOUBLE,
# MAGIC     min_offer_price_usd DOUBLE,
# MAGIC
# MAGIC     actual_currency STRING,
# MAGIC
# MAGIC     product_star_rating DOUBLE,
# MAGIC
# MAGIC     product_num_ratings DOUBLE,
# MAGIC     product_num_offers INT,
# MAGIC
# MAGIC     sales_volume_numeric INT
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (snapshot_date)
# MAGIC LOCATION 's3://rabboni-case-study-data/gold/fact_product_snapshot/';