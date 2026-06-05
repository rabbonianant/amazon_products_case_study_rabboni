# Databricks notebook source
import boto3
import json

# COMMAND ----------

# MAGIC %run /Workspace/Users/rabboni.anant@coditas.com/Case_Study/notebooks/Config/config

# COMMAND ----------

lambda_client = boto3.client(
    "lambda",
    aws_access_key_id=REDSHIFT["AWS_ACCESS_KEY"],
    aws_secret_access_key=REDSHIFT["AWS_SECRET_KEY"],
    region_name="ap-south-1"
)

TABLES = REDSHIFT["TABLES"]
BASE_PATH = REDSHIFT["COPY_BASE_PATH"]

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