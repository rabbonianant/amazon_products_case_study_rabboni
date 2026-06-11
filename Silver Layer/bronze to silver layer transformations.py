# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta, date
import re
from pyspark.sql.window import Window
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

def get_latest_timestamp():
    tracker_df = spark.table(SILVER_TRACKER_TABLE)
    latest_timestamp = tracker_df.filter((F.col("source") == "bronze") &(F.col("status") == "success"))\
        .agg(F.max("timestamp"))\
            .collect()[0][0]
    
    latest_date = tracker_df.filter((F.col("source") == "bronze") &(F.col("status") == "success"))\
        .agg(F.max("ingestion_date"))\
            .collect()[0][0]
    
    return latest_timestamp, latest_date

# COMMAND ----------

def get_incremental_api_df():
    latest_timestamp, latest_date = get_latest_timestamp()

    if latest_timestamp is None:
        return spark.read.json(
            "s3://rabboni-case-study-data/bronze/source=api/"
        )

    today = datetime.utcnow().date()
    paths = []
    current_date = latest_date

    while current_date <= today:
        paths.append(
            f"s3://rabboni-case-study-data/bronze/source=api/"
            f"ingestion_date={current_date.strftime('%Y-%m-%d')}/"
        )
        current_date += timedelta(days=1)

    try:
        df = spark.read.json(paths)
    except Exception:
        return None

    df = df.withColumn("file_run_timestamp",F.to_timestamp(F.col("metadata.run_timestamp"),"yyyy-MM-dd'T'HH-mm-ss'Z'"))\
        .filter(F.col("file_run_timestamp") > F.lit(latest_timestamp))
    
    return df

# COMMAND ----------

def get_incremental_csv_df():
    latest_timestamp, latest_date = get_latest_timestamp()

    if latest_timestamp is None:
        return (
            spark.read
            .option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .option("quote", '"')
            .csv("s3://rabboni-case-study-data/bronze/source=csv/")
        )

    today = datetime.utcnow().date()

    paths = []
    current_date = latest_date

    while current_date <= today:
        paths.append(
            f"s3://rabboni-case-study-data/bronze/source=csv/"
            f"ingestion_date={current_date.strftime('%Y-%m-%d')}/"
        )
        current_date += timedelta(days=1)

    try:
        df = (
            spark.read
            .option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .option("quote", '"')
            .csv(paths)
        )
    except Exception:
        return None

    df = df.withColumn("file_run_timestamp",F.to_timestamp(F.col("run"),"yyyy-MM-dd'T'HH-mm-ss'Z'"))\
        .filter(F.col("file_run_timestamp") > F.lit(latest_timestamp))

    return df

# COMMAND ----------

api_json_df = get_incremental_api_df()
csv_df = get_incremental_csv_df()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Flattening

# COMMAND ----------

def flatten_df(df):
    nested_columns = True

    while nested_columns:
        nested_columns = False
        for field in df.schema.fields:
            name = field.name
            dtype = field.dataType
            if isinstance(dtype, StructType):
                expanded = [
                    F.col(f"{name}.{nested.name}").alias(f"{name}_{nested.name}")
                    for nested in dtype.fields
                    ]

                df = df.select(
                    *[F.col(c) for c in df.columns if c != name],
                    *expanded
                    )

                nested_columns = True
                break
            elif isinstance(dtype, ArrayType):
                df = df.select(
                    *[F.col(c) for c in df.columns if c != name],
                    F.posexplode_outer(F.col(name)).alias(
                        f"{name}_position",
                        name
                    )
                )
                df = df.withColumn(
                    f"{name}_rank",
                    F.col(f"{name}_position") + 1
                )
                nested_columns = True
                break
    return df

# COMMAND ----------

flattened_api_df = flatten_df(api_json_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize Column Names

# COMMAND ----------

standard_columns = SILVER["STANDARD_COLUMNS"]
api_df = flattened_api_df.withColumnsRenamed(standard_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Only Useful Columns

# COMMAND ----------

required_columns = SILVER["REQUIRED_COLUMNS"]

api_df_standardized = api_df.select(*[column for column in required_columns if column in api_df.columns])
csv_df = csv_df.select(*[column for column in required_columns if column in csv_df.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Datasets

# COMMAND ----------

combined_data_df = api_df_standardized.unionByName(csv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Values and Typecast 

# COMMAND ----------

combined_data_df = combined_data_df.withColumn("detected_currency", F.regexp_extract(F.col("product_price"), r"([A-Z]{3}|₹|\$|€|£)", 1))\
    .withColumn("actual_currency",F.when(F.col("detected_currency") == "$", "USD").otherwise(F.col("detected_currency")))

# COMMAND ----------

# Typecasting
columns = combined_data_df.columns
for column_name in SILVER["CURRENCY_COLUMNS"]:
    if column_name in columns:
        combined_data_df = combined_data_df.withColumn(
            column_name,
            F.regexp_replace(
                F.col(column_name),
                r"[^0-9.]",
                ""
            )
        )

for column_name, target_type in SILVER["TYPE_CAST_COLUMNS"].items():
    if column_name in columns:
        combined_data_df = combined_data_df.withColumn(
            column_name,
            F.col(column_name).cast(target_type)
        )   




# COMMAND ----------


typecasted_data_df = combined_data_df.withColumn("run_timestamp",F.to_timestamp(F.col("run"),"yyyy-MM-dd'T'HH-mm-ss'Z'"))\
    .drop(F.col("run"))

# COMMAND ----------

typecasted_data_df = typecasted_data_df.withColumn(
    "sales_volume_numeric",
    F.when(
        F.col("sales_volume").rlike(r"[0-9.]+K"),
        (
            F.expr(
                "try_cast(regexp_extract(sales_volume, '([0-9.]+)', 1) as double)"
            ) * 1000
        ).cast("int")
    ).when(
        F.col("sales_volume").rlike(r"[0-9]+"),
        F.expr(
            "try_cast(regexp_extract(sales_volume, '([0-9]+)', 1) as int)"
        )
    ).otherwise(None)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null Handling

# COMMAND ----------


boolean_null_handling = {
    field.name : False
    for field in typecasted_data_df.schema.fields if isinstance(field.dataType, BooleanType)
}
cleaned_data_df = typecasted_data_df.fillna(boolean_null_handling)\
    .fillna(SILVER["NULL_HANDLING"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

window = SILVER["DEDUPLICATION_WINDOW"]

silver_data_df = cleaned_data_df.withColumn("row_number", F.row_number().over(window))\
    .filter(F.col("row_number") == 1)\
        .drop("row_number")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Global Ranking

# COMMAND ----------

window = SILVER["SEARCH_RANK_WINDOW"]
silver_data_df = silver_data_df.withColumn("rank_by_keyword", F.row_number().over(window))


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write to silver table

# COMMAND ----------

new_latest_timestamp = silver_data_df.select(F.max("run_timestamp")).collect()[0][0]
new_latest_date = silver_data_df.select(F.max("date")).collect()[0][0]

try:
    silver_data_df.write.mode("append")\
        .option("mergeSchema", "true")\
            .format("delta")\
                .partitionBy("date")\
                    .save(SILVER["WRITE_PATH"])

    tracker_row = [Row(
        source="bronze",
        layer="silver",
        status="success",
        timestamp=new_latest_timestamp,
        ingestion_date=new_latest_date,
        total_records=silver_data_df.count(),
    )]
except Exception as e:
    tracker_row = [Row(
        source="bronze",
        layer="silver",
        status="failed",
        timestamp=datetime.now(),
        ingestion_date=date.today(),
        total_records=0,
    )]
    raise e
finally:
    tracker_schema = spark.table(SILVER_TRACKER_TABLE).schema
    tracker_df = spark.createDataFrame(tracker_row, schema=tracker_schema)
    tracker_df.write.mode("append").format("delta").saveAsTable(SILVER_TRACKER_TABLE)