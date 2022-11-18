from os import environ
from pyspark import sql, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from py4j.java_gateway import java_import
import time
import configparser
import socket
import pandas as pd
pd.options.display.max_colwidth = 100
# Full url of the Nessie API endpoint to nessie
url = "<catalog endpoint >"
# Where to store nessie tables
full_path_to_warehouse = "<s3 path of your arctic catalog>"
# The ref or context that nessie will operate on (if different from default branch).
# Can be the name of a Nessie branch or tag name.
ref = "main"
# Nessie authentication type (BASIC, NONE or AWS)
auth_type = "BEARER"
token = "i<PAT>"

# here we are assuming NONE authorisation
spark = SparkSession.builder \
        .config("spark.sql.catalog.nessie",
            "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse) \
        .config("spark.sql.catalog.nessie.catalog-impl", 
            "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.io-impl" , "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.nessie.uri", url) \
        .config("spark.sql.catalog.nessie.ref", ref) \
        .config("spark.sql.catalog.nessie.authentication.type", auth_type) \
        .config("spark.sql.catalog.nessie.authentication.token", token)  \
        .config("spark.sql.catalog.nessie.cache-enabled", "false")  \
        .getOrCreate()


spark = SparkSession.builder.getOrCreate()
spark.sql("use nessie")

################################################
#                                              #
#      Merge branch emp_etl to main            #
#                                              #
################################################


# Switch to the ETL branch
spark.sql("USE REFERENCE emp_etl IN nessie")

# Merge the changes to main and drop the elt branch
spark.sql("MERGE BRANCH emp_etl INTO main IN nessie")

