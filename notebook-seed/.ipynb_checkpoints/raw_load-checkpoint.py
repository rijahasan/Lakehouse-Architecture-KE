# #172.18.0.4
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
# import os

# CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
# WAREHOUSE = "s3://warehouse/"               # Minio Address to Write to
# STORAGE_URI = "http://172.18.0.3:9000"     # Minio IP address from docker inspect

# conf = (
#     pyspark.SparkConf()
#         .setAppName('billing')
#         # Include necessary packages
#         .set("spark.sql.debug.maxToStringFields", "100000")
#         .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8')
#         # Enable Iceberg and Nessie extensions
#     # org.postgresql:postgresql:42.7.3,
#         .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
#         # Configure Nessie catalog
#         .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
#         .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
#         .set('spark.sql.catalog.nessie.ref', 'main')
#         .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
#         .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
#         # Set Minio as the S3 endpoint for Iceberg storage
#         .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
#         .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
#         .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
# )

# spark = SparkSession.builder.config(conf=conf).getOrCreate()
# print("Spark Session Started")

import logging
import sys
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import re
from pyspark.sql.functions import lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("[INFO] Incremental load script started...")
    # Your data processing logic here...
    # logging.info("[INFO] Processing file: " + sys.argv[1])
    logging.info("[INFO] Processing file: " + sys.argv[1])
    CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
    WAREHOUSE = "s3://warehouse/"               # Minio Address to Write to
    STORAGE_URI = "http://172.18.0.3:9000"     # Minio IP address from docker inspect
    try:
        conf = (
            pyspark.SparkConf()
                .setAppName('billing')
                .set("spark.sql.debug.maxToStringFields", "100000")
                .set('spark.jars', '/opt/spark/workjars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/spark/workjars/nessie-spark-extensions-3.5_2.12-0.77.1.jar,/opt/spark/workjars/bundle-2.24.8.jar,/opt/spark/workjars/url-connection-client-2.24.8.jar')
                .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
                .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
                .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
                .set('spark.sql.catalog.nessie.ref', 'main')
                .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
                .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
                .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
                .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
                .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memoryOverhead", "512m")
                .set("spark.sql.shuffle.partitions", "64")
                .set("spark.shuffle.spill", "true")
                .set("spark.shuffle.memoryFraction", "0.4"))
        spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()
        logging.info("Spark Session Started")
    except Exception as e:
        logging.info(f"Spark Session could not be started: {str(e)}")
        sys.exit(1)  # Non-zero exit code indicates failure
        signal_file = "/mnt/inc_processed/done.signal"
        if not os.path.exists(signal_file):
            with open(signal_file, "w") as f:
                f.write("DONE")        
    # csv = "/mnt/HabibData/"+sys.argv[1]
    csv = sys.argv[1]
    # Read the CSV file into a DataFrame
    match = re.search(r'_(\d{6})', csv)
    billing_month = match.group(1) if match else "Unknown"
    df = spark.read \
        .option("delimiter", "|") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv)
    # Add the BillingMonth column
    df = df.withColumn("BillingMonth", lit(billing_month))
    # Repartition the DataFrame
    # df = df.limit(1)
    df = df.repartition(200)
    result = spark.sql("SHOW TABLES IN nessie").collect()

    table = 'ageing'
    try:
        if table in [r.namespace for r  in result]:
            logging.info("Table exists, proceeding with appending data.")
            df.writeTo(f"nessie.{table}.{table}_data_raw").append()
        else:
            logging.info("Table does not exist, creating one")
            spark.sql(f"CREATE NAMESPACE nessie.{table}").show()
            df.writeTo(f"nessie.{table}.{table}_data_raw").createOrReplace()        
    except Exception as e:
        logging.info(f"Data could not be added: {str(e)}")
    spark.stop()
    logging.info("Spark Session Stopped")
    logging.info("[INFO] Incremental load script completed successfully!")

    # Create done.signal in the new directory
    signal_file = "/mnt/inc_processed/done.signal"
    if os.path.exists(signal_file):
        with open(signal_file, "w") as f:
            f.write("DONE")
    else:
        logging.info("[INFO] done.signal already exists. Skipping creation.")
        signal_file = "/mnt/inc_processed/done.signal"
        if os.path.exists(signal_file):
            with open(signal_file, "w") as f:
                f.write("DONE")

if __name__ == "__main__":
    main()
