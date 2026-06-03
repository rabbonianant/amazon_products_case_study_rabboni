# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

SILVER_TRACKER_TABLE = "amazon_case_study_silver_tracker" 
GOLD_TRACKER_TABLE = "amazon_case_study_gold_tracker"

# COMMAND ----------

SILVER = {
    "DEDUPLICATION_WINDOW": Window.partitionBy("asin","search_term","run_timestamp").orderBy(F.col("run_timestamp").desc()),
    "SEARCH_RANK_WINDOW": Window.partitionBy("page","search_term","run_timestamp").orderBy(F.col("page_position").asc()),
    "WRITE_PATH": "s3://rabboni-case-study-data/silver/products_table/",

    "STANDARD_COLUMNS": {
        "data_products_product_title": "product_title",
        "data_products_asin": "asin",
        "data_products_rank":"page_position",
        "data_products_product_price": "product_price",
        "data_products_product_original_price": "product_original_price",
        "data_products_currency": "currency",
        "data_products_product_star_rating": "product_star_rating",
        "data_products_product_num_ratings": "product_num_ratings",
        "data_products_product_url": "product_url",
        "data_products_product_photo": "product_photo",
        "data_products_product_num_offers": "product_num_offers",
        "data_products_product_minimum_offer_price": "product_minimum_offer_price",
        "data_products_is_best_seller": "is_best_seller",
        "data_products_is_amazon_choice": "is_amazon_choice",
        "data_products_is_prime": "is_prime",
        "data_products_climate_pledge_friendly": "climate_pledge_friendly",
        "data_products_sales_volume": "sales_volume",
        "data_products_delivery": "delivery",
        "data_products_has_variations": "has_variations",
        "metadata_search_term": "search_term",
        "metadata_page_number": "page",
        "metadata_ingestion_date": "date",
        "metadata_run_timestamp": "run", 
    },
    "REQUIRED_COLUMNS": [
        "asin",
        "product_title",
        "page_position",
        "page",
        "product_price",
        "product_original_price",
        "currency",
        "product_star_rating",
        "product_num_ratings",
        "product_url",
        "product_photo",
        "product_num_offers",
        "product_minimum_offer_price",
        "is_best_seller",
        "is_amazon_choice",
        "is_prime",
        "climate_pledge_friendly",
        "sales_volume",
        "delivery",
        "has_variations",
        "search_term",
        "date",
        "run"
    ],
    "NULL_HANDLING":{
        "product_num_ratings": 0,
        "sales_volume": 0,
        "is_amazon_choice": False,
        "is_best_seller": False,
        "is_prime": False,
        "actual_currency": "UNKNOWN"
    },    
    "CURRENCY_COLUMNS": ["product_price","product_original_price","product_minimum_offer_price"],
    "TYPE_CAST_COLUMNS": {
        #double
        "product_price": "double",
        "product_original_price": "double",
        "product_minimum_offer_price": "double",
        "product_star_rating": "double",
        "product_num_ratings":"double",
        #int
        "product_num_offers": "int",
        "page_position":"int",
        "page":"int",
        "sales_volume_numeric":"int",
        "rank_by_keyword":"int",
        #ts
        "run_timestamp": "timestamp",
        "date":"date"
    }
}

# COMMAND ----------

GOLD = {
    'DIM_PRODUCT_COLUMNS': ['asin', 'product_title', 'product_url', 'product_photo', 'has_variations', 'run_timestamp']
}