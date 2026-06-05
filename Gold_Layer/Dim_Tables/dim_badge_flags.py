# Databricks notebook source
import pyspark.sql.functions as F
from itertools import product

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

def create_dim_badge_flags(flag_columns):
    flag_names = list(flag_columns.keys())
    rows = []
    for combination in product([False, True], repeat=len(flag_names)):
        enabled_flags = [
            flag_columns[flag_name]
            for flag_name, is_enabled in zip(flag_names, combination)
            if is_enabled
        ]
        badge_combination = (
            " + ".join(enabled_flags)
            if enabled_flags
            else "None"
        )
        rows.append(
            tuple(combination) + (badge_combination,)
        )
    df = spark.createDataFrame(
        rows,
        schema=flag_names + ["badge_combination"]
    )
    return (
        df.withColumn(
            "badge_key",
            F.abs(F.xxhash64("badge_combination"))
        )
        .select(
            "badge_key",
            *flag_names,
            "badge_combination"
        )
    )

# COMMAND ----------

dim_badge_flags_df = create_dim_badge_flags(GOLD["DIM_BADGE_FLAG_COLUMNS"])

# COMMAND ----------

dim_badge_flags_df.write\
    .mode("overwrite")\
        .option("mergeSchema", "true")\
            .option("overwriteSchema", "true")\
                .format("delta")\
                    .saveAsTable(GOLD["DIM_BADGE_FLAGS"])
