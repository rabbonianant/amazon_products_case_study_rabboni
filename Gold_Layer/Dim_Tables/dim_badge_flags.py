# Databricks notebook source
import pyspark.sql.functions as F
from itertools import product

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

def create_dim_badge_flags():
    rows = []
    for best_seller, amazon_choice, prime, climate_pledge in product([False, True], repeat=4):
        badges = []
        if best_seller:
            badges.append("Best Seller")
        if amazon_choice:
            badges.append("Amazon Choice")
        if prime:
            badges.append("Prime")
        if climate_pledge:
            badges.append("Climate_Pledge")

        badge_combination = " + ".join(badges) if badges else "None"
        rows.append((best_seller,amazon_choice,prime,climate_pledge,badge_combination))

    return spark.createDataFrame(
        rows,
        schema=[
            "is_best_seller",
            "is_amazon_choice",
            "is_prime",
            "climate_pledge",
            "badge_combination"
        ]
    )

# COMMAND ----------

dim_badge_flags_df = create_dim_badge_flags()

# COMMAND ----------

dim_badge_flags_df.write\
    .mode("overwrite")\
        .format("delta")\
            .option("replaceWhere", "1=1")\
                .saveAsTable("gold.dim_badge_flags")