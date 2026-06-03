# Databricks notebook source
# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

from pyspark.sql import functions as F

def write_with_tracker(
    df,
    target_table,
    table_name,
    tracker_table,
    tracker_schema,
    processed_timestamp,
):

    records_written = df.count()

    try:

        if records_written > 0:
            if table_name == 'fact_product_snapshot':
                (
                df.write
                .format("delta")
                .mode("append")
                .partitionBy("snapshot_date")
                .saveAsTable(target_table)
            )
            else:
                (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )

        tracker_record = spark.createDataFrame(
            [
                (
                    table_name,
                    "gold",
                    "success",
                    processed_timestamp,
                    processed_timestamp.date(),
                    records_written
                )
            ],
            schema=tracker_schema
        )

        (
            tracker_record.write
            .format("delta")
            .mode("append")
            .saveAsTable(tracker_table)
        )

    except Exception as e:

        tracker_record = spark.createDataFrame(
            [
                (
                    table_name,
                    "gold",
                    "failed",
                    processed_timestamp,
                    processed_timestamp.date(),
                    0
                )
            ],
            schema=tracker_schema
        )

        (
            tracker_record.write
            .format("delta")
            .mode("append")
            .saveAsTable(tracker_table)
        )

        raise e

# COMMAND ----------

def get_latest_gold_timestamp(table_name):
    tracker_df = spark.table(GOLD_TRACKER_TABLE)

    filtered_df = tracker_df.filter(
        (F.col("table_name") == table_name) &
        (F.col("status") == "success")
    )

    latest_timestamp = (
        filtered_df
        .agg(F.max("timestamp").alias("latest_timestamp"))
        .collect()[0]["latest_timestamp"]
    )

    latest_date = (
        filtered_df
        .agg(F.max("processing_date").alias("latest_date"))
        .collect()[0]["latest_date"]
    )

    return latest_timestamp, latest_date