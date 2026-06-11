# Databricks notebook source
# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

from pyspark.sql import functions as F

def write_with_tracker(
    df,
    target,
    table_name,
    tracker_table,
    tracker_schema,
    processed_timestamp,
    write_type="delta",
    partition_columns=None
):

    records_written = df.count()
    try:
        if records_written > 0:
            writer = df.write.mode("append")

            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            if write_type == "delta":
                writer.format("delta").saveAsTable(target)
            
            elif write_type == "parquet":
                writer.parquet(target)

            else:
                raise ValueError(
                    f"Unsupported write_type: {write_type}"
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