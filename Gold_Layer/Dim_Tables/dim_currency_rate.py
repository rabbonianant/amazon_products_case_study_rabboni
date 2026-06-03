# Databricks notebook source
# MAGIC %sql
# MAGIC describe gold.dim_currency_rate

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_currency_rate
# MAGIC (
# MAGIC     currency_code,
# MAGIC     currency_name,
# MAGIC     usd_conversion_rate,
# MAGIC     is_active
# MAGIC )
# MAGIC VALUES
# MAGIC
# MAGIC ('USD', 'US Dollar',               1.0000, TRUE),
# MAGIC ('EUR', 'Euro',                    1.1300, TRUE),
# MAGIC ('GBP', 'British Pound',           1.3400, TRUE),
# MAGIC ('INR', 'Indian Rupee',            0.0120, TRUE),
# MAGIC ('JPY', 'Japanese Yen',            0.0069, TRUE),
# MAGIC ('CAD', 'Canadian Dollar',         0.7300, TRUE),
# MAGIC ('AED', 'UAE Dirham',              0.2720, TRUE),
# MAGIC ('HKD', 'Hong Kong Dollar',        0.1280, TRUE),
# MAGIC ('ILS', 'Israeli Shekel',          0.2800, TRUE),
# MAGIC ('MXN', 'Mexican Peso',            0.0520, TRUE),
# MAGIC ('BRL', 'Brazilian Real',          0.1800, TRUE),
# MAGIC ('PLN', 'Polish Zloty',            0.2700, TRUE),
# MAGIC ('RON', 'Romanian Leu',            0.2200, TRUE),
# MAGIC ('TRY', 'Turkish Lira',            0.0250, TRUE),
# MAGIC ('UNKNOWN','Unknown Currency',NULL,TRUE);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_currency_rate