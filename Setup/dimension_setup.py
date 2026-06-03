# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_product (
# MAGIC     product_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     asin STRING,
# MAGIC     product_title STRING,
# MAGIC     product_url STRING,
# MAGIC     product_photo STRING,
# MAGIC
# MAGIC     has_variations BOOLEAN,
# MAGIC
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_upto TIMESTAMP,
# MAGIC     is_current BOOLEAN,
# MAGIC
# MAGIC     hash_value STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://rabboni-case-study-data/gold/dim_product/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_search_term (
# MAGIC     search_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     search_term STRING,
# MAGIC     keyword_count INT
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://rabboni-case-study-data/gold/dim_search_term/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_date (
# MAGIC
# MAGIC     date_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     date DATE,
# MAGIC
# MAGIC     year INT,
# MAGIC     quarter INT,
# MAGIC
# MAGIC     month INT,
# MAGIC     month_name STRING,
# MAGIC
# MAGIC     week INT,
# MAGIC     week_of_year INT,
# MAGIC
# MAGIC     day INT,
# MAGIC     day_of_year INT,
# MAGIC
# MAGIC     day_of_week STRING,
# MAGIC     day_of_week_num INT,
# MAGIC
# MAGIC
# MAGIC     is_weekend BOOLEAN,
# MAGIC     is_month_start BOOLEAN,
# MAGIC     is_month_end BOOLEAN,
# MAGIC     is_quarter_start BOOLEAN,
# MAGIC     is_quarter_end BOOLEAN
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://rabboni-case-study-data/gold/dim_date/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_badge_flags (
# MAGIC     badge_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     is_best_seller BOOLEAN,
# MAGIC     is_amazon_choice BOOLEAN,
# MAGIC     is_prime BOOLEAN,
# MAGIC     climate_pledge BOOLEAN,
# MAGIC
# MAGIC     badge_combination STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://rabboni-case-study-data/gold/dim_badge_flags/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_currency_rate (
# MAGIC
# MAGIC     currency_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC
# MAGIC     currency_code STRING,
# MAGIC     currency_name STRING,
# MAGIC
# MAGIC     usd_conversion_rate DOUBLE,
# MAGIC
# MAGIC     is_active BOOLEAN
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://rabboni-case-study-data/gold/dim_currency_rate/';