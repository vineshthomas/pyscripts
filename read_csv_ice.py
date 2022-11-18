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
# Read csv files and create an iceberg table #
#                                              #
################################################

#First cleanup employee_info table
spark.sql("DROP TABLE IF EXISTS Zillow.emp.employee_info")

# Create a ETL branch
spark.sql("CREATE BRANCH IF NOT EXISTS emp_etl IN nessie")

# Switch to the ETL branch
spark.sql("USE REFERENCE emp_etl IN nessie")

employee_df = spark.read.csv("s3a://vt-bucket-2/employees/employees.csv",header=True)

employee_df.writeTo("Zillow.emp.employee_info").using("iceberg").create()


emp_df = spark.table("Zillow.emp.employee_info")
emp_df.show()


