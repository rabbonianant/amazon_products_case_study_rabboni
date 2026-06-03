# Databricks notebook source
import boto3
import json

# COMMAND ----------

lambda_client = boto3.client(
    "lambda",
    aws_access_key_id="****", 
    aws_secret_access_key="**********",
    region_name="ap-south-1"
)

TABLES = ["product_visibility_daily"]
BASE_PATH = "s3://rabboni-case-study-data/redshift_exports"

for table in TABLES:
    export_path = f"{BASE_PATH}/{table}/export/"
    temp_path = f"{BASE_PATH}/{table}/temp/"

    payload = {
        "table": table,
        "s3_path": export_path
    }

    response = lambda_client.invoke(
        FunctionName="trigger-redshift-copy",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )

    result = json.loads(response["Payload"].read())

    print(f"{table}: {result}")
    if result.get("state") == "FINISHED":

        dbutils.fs.rm(temp_path, recurse=True)
        dbutils.fs.rm(export_path, recurse=True)
        print(f"Cleaned folders for {table}")

    else:
        raise Exception(
            f"Redshift load failed for {table}: {result}"
        )