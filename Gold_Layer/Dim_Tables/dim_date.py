# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

dates = spark.range(1).select(
    F.explode(
        F.sequence(
            F.lit(GOLD["DIM_DATE_BORDER"]["START_DATE"]).cast("date"),
            F.lit(GOLD["DIM_DATE_BORDER"]["END_DATE"]).cast("date"),
            F.expr("interval 1 day")
        )
    ).alias("date")
)

dates.display()

# COMMAND ----------

dates = dates.withColumn("year", F.year("date"))\
    .withColumn("quarter", F.quarter("date"))\
    .withColumn("month", F.month("date"))\
    .withColumn("month_name", F.monthname("date"))\
    .withColumn("week", F.weekofyear("date"))\
    .withColumn("week_of_year", F.weekofyear("date"))\
    .withColumn("day", F.dayofmonth("date"))\
    .withColumn("day_of_year", F.dayofyear("date"))\
    .withColumn("day_of_week", F.date_format("date", "E"))\
    .withColumn("day_of_week_num", F.dayofweek("date"))\
    .withColumn("is_weekend", F.when(F.dayofweek("date") == 1, True).when(F.dayofweek("date") == 7, True).otherwise(False))\
    .withColumn("is_month_start", F.when(F.col("day") == 1, True).otherwise(False))\
    .withColumn("is_month_end", F.when(F.last_day("date") == F.col("date"), True).otherwise(False))\
    .withColumn("is_quarter_start",F.when(F.col("month").isin(1, 4, 7, 10) &(F.col("day") == 1),True).otherwise(False))\
    .withColumn("is_quarter_end",F.when(F.col("month").isin(3, 6, 9, 12) &(F.last_day("date") == F.col("date")),True).otherwise(False))
    

# COMMAND ----------

dates.write.mode("overwrite").format("delta").option("replaceWhere", "1=1").saveAsTable(GOLD["DIM_DATE"])