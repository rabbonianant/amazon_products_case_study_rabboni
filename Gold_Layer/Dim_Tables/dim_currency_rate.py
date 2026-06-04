# Databricks notebook source
# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

def create_dim_currency(currency_config):
    rows = []

    for currency_code, values in currency_config.items():
        currency_name, usd_conversion_rate = values
        rows.append(
            (
                currency_code,
                currency_name,
                usd_conversion_rate,
                True
            )
        )

    return spark.createDataFrame(
        rows,
        schema=[
            "currency_code",
            "currency_name",
            "usd_conversion_rate",
            "is_active"
        ]
    )

# COMMAND ----------

dim_currency_df = create_dim_currency(GOLD["DIM_CURRENCY"])

# COMMAND ----------

dim_currency_df.write\
    .format("delta")\
        .mode("overwrite")\
            .option("overwriteSchema", "true")\
                .saveAsTable('gold.dim_currency_rate')
