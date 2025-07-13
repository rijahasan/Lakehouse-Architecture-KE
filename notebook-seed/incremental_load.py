import logging
import sys
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


logger = logging.getLogger("py4j")  # Optional: suppress py4j logs if too noisy
logger.setLevel(logging.WARNING)

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[
    logging.StreamHandler(sys.stdout)
])

def replace_invalid_strings_with_null(df):
    # Iterate over all columns in the DataFrame
    for col_name, dtype in df.dtypes:
        # If the column is of string type, replace invalid values with null
        if dtype == 'string':
            df = df.withColumn(
                col_name,
                F.when(
                    (F.col(col_name) == '.') | 
                    (F.col(col_name) == '-') | 
                    (F.col(col_name) == 'Undefined') | 
                    (F.col(col_name) == 'Not found'),
                    F.lit(None)  # Replace with null
                ).otherwise(F.col(col_name))  # Keep the original value if valid
            )    
    return df

def main():
    logging.info("[INFO] Incremental load script started...")
    # Your data processing logic here...
    # logging.info("[INFO] Processing file: " + sys.argv[1])
    # ---------------------------------------
    
    logging.info("[INFO] Processing file: " + sys.argv[1])
    CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
    WAREHOUSE = "s3://warehouse/"               # Minio Address to Write to
    STORAGE_URI = "http://172.18.0.3:9000"     # Minio IP address from docker inspect
    try:
        conf = (
            pyspark.SparkConf()
        .setAppName('billing')
        # Include necessary packages
        .set("spark.sql.debug.maxToStringFields", "100000")
        .set('spark.jars', '''/opt/spark/workjars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,
        /opt/spark/workjars/nessie-spark-extensions-3.5_2.12-0.77.1.jar,/opt/spark/workjars/bundle-2.24.8.jar,
        /opt/spark/workjars/url-connection-client-2.24.8.jar''')
        .set('spark.sql.extensions', '''org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,
        org.projectnessie.spark.extensions.NessieSparkSessionExtensions''')
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
        .set("spark.shuffle.memoryFraction", "0.4")
        .set("spark.ui.showConsoleProgress", "false")
        )
        spark = SparkSession.builder.config(conf=conf).getOrCreate()        
        logging.info("Spark Session Started")
        spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        logging.info(f"Spark Session could not be started: {str(e)}")
        sys.exit(1)  # Non-zero exit code indicates failure
        signal_file = "/mnt/inc_processed/signals/done.signal"
        if not os.path.exists(signal_file):
            with open(signal_file, "w") as f:
                f.write("DONE")        
    # csv = "/mnt/HabibData/"+sys.argv[1]
    csv = sys.argv[1]
    # Read the CSV file into a DataFrame
    import time
    timedict = {}
    starttime = time.time()
    df = spark.read \
        .option("delimiter", "|") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv)
    timetaken = time.time()-starttime
    logging.info("Time taken to read through spark: %.2f seconds", timetaken)
    timedict["Reading csv"] = timetaken
    result = spark.sql("SHOW TABLES IN nessie").collect()
    import re
    from pyspark.sql.functions import lit    
    table = 'ageing_check'
    match = re.search(r'_(\d{6})', csv)#adding billing month from csv
    billing_month = match.group(1) if match else "Unknown"
    df = df.withColumn("BillingMonth", lit(billing_month))
    
    # df = df.repartition(200)    # Repartition the DataFrame - no needed anymore
    # choosing not to add raw data 
    # try:
    #     if table in [r.namespace for r  in result]:
    #         logging.info(f"Table {table} exists, proceeding with appending data.")
            # df.writeTo(f"nessie.{table}.{table}_data_raw").append()
    #     else:
    #         logging.info(f"Table {table} does not exist, creating one")
    #         spark.sql(f"CREATE NAMESPACE nessie.{table}").show()
    #         df.writeTo(f"nessie.{table}.{table}_data_raw").createOrReplace()        
    # except Exception as e:
    #     logging.info(f"Data could not be added: {str(e)}")

        # Cleaning Ageing
        # choose only status = 'ACT', transform all date columns, handle nullvalues (for integers check null)
    
        # Assuming df is your DataFrame, and SparkSession is already initialized
    # spark = SparkSession.builder.appName("RemoveColumns").getOrCreate()
    
    # List of columns to remove from the DataFrame
    
    #----------------------- cleaning started
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
    import re
    from pyspark.sql.functions import to_date, col, lpad, lit, concat, to_timestamp, regexp_replace, col, to_date, year, month, sum as _sum, date_format
    from pyspark.sql import functions as F
    from pyspark.sql.types import FloatType

    columns_to_remove = [
        'A <=(366)', 'B (365)-(1)', 'C 0-30', 'D 31-60', 'E 61-90', 'F 91-120', 'G 121-150',
        'H 151-180', 'I 181-210', 'J 211-240', 'K 241-270', 'L 271-300', 'M 301-330', 'N 331-360',
        'O 361-390', 'P 391-420', 'Q 421-450', 'R 451-480', 'S 481-510', 'T 511-540', 'U 541-570',
        'V 571-600', 'W 601-630', 'X 631-660', 'Y 1021-1050', 'Y 1051-1080', 'Y 1081-1110', 'Y 1111-1140',
        'Y 1141-1170', 'Y 1171-1200', 'Y 1201-1230', 'Y 1231-1260', 'Y 1261-1460', 'Y 1461-1825',
        'Y 661-690', 'Y 691-720', 'Y 721-750', 'Y 751-780', 'Y 781-810', 'Y 811-840', 'Y 841-870',
        'Y 871-900', 'Y 901-930', 'Y 931-960', 'Y 961-990', 'Y 991-1020', 'Z >=1826', 'Key Date',
        'Legacy Account Number', 'Legacy Move In Date', 'Contract', 'Move-In Date', 'Move-Out Date',
        'PIBC', 'DCIBC', 'DC OIP', 'DC Rate Category', 'CA Creation Date', 'Contract Creation Date',
        'ConsumerCounter', 'Last SIR Number', 'Last SIR CreatedOn', 'AverageUnits',
        'AverageAmount', 'AdjustedUnits', 'AdjustedAmount', 'AssessedUnits', 'AssessedAmount', 'FICAMonth',
        'OpeningBalance', 'MigrationBalance', 'BankCharges', 'WriteOff', 'PreviousYearAllowance',
        'DownPaymentRequest', 'DownPayment', 'OutstandingDownPayment', 'MNCVAdjustment', 'MNCVPayment',
        'MNCVClearingAmount', 'Set Aside Amount', 'Set Aside Code', 'Installement Number', 'Installement Amount',
        'Agency', 'Schedule Number', 'PSC Consumer IBC', 'Adjustment', 'IBCTransfer', 'ClearingAmount', '',
        'PSC Location','PSC Department','PSC Classification','PSC Ministry','PSC Sub-Department'
        
    ]
    df_reduced = df.drop(*columns_to_remove)    # Drop the columns from the DataFrame
    df_filtered = df_reduced.filter(df_reduced.Status == 'ACT')
    df_filtered = df_filtered.drop('Status')
    df_updated = df_filtered.withColumn('Last Payment Date', to_timestamp(col('Last Payment Date'), 'dd-MMM-yy'))
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") #important warna error
    # NULL values
    df_updated = replace_invalid_strings_with_null(df_updated)
    # df_updated.filter(df_updated["Region"] == "Undefined").limit(1).show()
    # Update df_updated by adding two new columns IRBAmount and IRBUnits
    df_updated = df_updated.withColumn(
        "IRBAmount", F.col("IRBRevisedAmount") + F.col("IRBDetectionAmount")
    ).withColumn(
        "IRBUnits", F.col("IRBRevisedUnits") + F.col("IRBDetectionUnits")
    )
    from pyspark.sql.functions import col, to_date, year, month, sum as _sum
    # Parse Last Payment Date to extract month and year for grouping
    df_updated = df_updated.withColumn("LastPaymentDate", col("Last Payment Date").cast("date"))
    df_updated = df_updated.withColumn("PaymentYear", year(col("LastPaymentDate")))
    df_updated = df_updated.withColumn("PaymentMonth", month(col("LastPaymentDate")))
    df_updated = df_updated.withColumn("PMT", col("PMT").cast("bigint"))
    # Assuming the 'billing_month' column is in the form YYYYMM as an integer
    df_updated = df_updated.withColumn(
        "Year", F.substring(F.col("billingmonth").cast("string"), 1, 4).cast("int")
    ).withColumn(
        "Month", F.substring(F.col("billingmonth").cast("string"), 5, 2).cast("int")
    )
    # Overwrite BillingMonth with a proper date (e.g., 2023-04-01)
    df_updated = df_updated.withColumn(
        "BillingMonth",
        to_date(concat(lpad(col("BillingMonth").cast("string"), 6, "0"), lit("01")), "yyyyMMdd")
    )
    logging.info("Cleaning Completed!")
    #----------------------- cleaning complete    
    #Appending to ageing_fact
    start = time.time()
    # df_updated.writeTo("nessie.starschema.ageing_fact").append()
    df_updated.limit(5).show(truncate=False)
    timetaken = time.time()-starttime
    cleanedcount=df_updated.count()
    logging.info(f"Records before cleaning: {df.count()}")
    logging.info(f"Records after cleaning: {cleanedcount}")
    logging.info("Time taken to clean and append batch: %.2f seconds", timetaken)
    
    timedict["Cleaning and ingesting ageing"] = timetaken
    time.sleep(1)

    #----------------------- Transformations and joins
    ageing_new_data = df_updated                              #for joins
    #if the month is wuthin the period where crm data is present, Transform
    if 4 <= int(billing_month[4:6]) <= 9:
        ageing_new_data = df_updated                              #for joins
        logging.info("Billing month has corresponding data in CRM from Apr-Sep")
    #----------------------- crm consumer join
        logging.info("Appending Ageing x CRM consumer dim")
        crm_consumer_dim_ = spark.read.table("nessie.starschema.crm_consumer_dim")
        consumer_ageing_crm_dim = ageing_new_data.alias("ageing").join(
            crm_consumer_dim_.alias("crm"), 
            (F.col("ageing.AccountContract") == F.col("crm.AccountContract")) &
            (F.col("ageing.Month") == F.col("crm.Month")) &
            (F.col("ageing.Year") == F.col("crm.Year")),
            how='inner'
        ).groupBy("ageing.AccountContract","ageing.Month","ageing.Year").agg(
            # First value of relevant Ageing columns (from ageing_new_data)
            F.first("ageing.BillingMonth").alias("Billing Month"),
            F.first("ageing.Business Partner").alias("Business_Partner"),
            F.first("ageing.Consumer Type").alias("ConsumerType"),
            F.first("ageing.Customer Name").alias("ConsumerName"),
            F.first("ageing.Postal Code").alias("PostalCode"),
            F.first("ageing.Billing Class").alias("BillingClass"),
            F.first("ageing.Phase").alias("Phase"),
            F.first("ageing.OIP").alias("OIP"),
            F.first("ageing.Premise Type").alias("Premise_Type"),
            F.first("ageing.IBCName").alias("IBCName"),
            F.first("ageing.IBC").alias("IBC_Code"),
            F.first("ageing.BCM").alias("Billing Charge Mode (BCM)"),
            # First value of relevant crm columns (from crm_consumer_dim)
            # F.first("crm.Month").alias("Month"), not needed cuz already contained in group by
            # F.first("crm.Year").alias("Year"),
            F.first("crm.StarCustomer").alias("StarCustomer"),
        
            # Not taking aggregates from ageing because each account contract is unique in a month
            F.first("ageing.IRBAmount").alias("IRB Amount"),
            F.first("ageing.IRBUnits").alias("IRB Units"),
            F.first("ageing.Sanctioned Load").alias("Sanctioned Load"),
            F.first("ageing.Connected Load").alias("Connected Load"),
            F.first("ageing.CurrentAmount").alias("Current Amount"),
            F.first("ageing.CurrentUnits").alias("Current Units"),
            
            # Aggregated values from CRM
            F.first("crm.Complaint Counts (Apr-Sep_23-24)").alias("Complaint Counts"),
            F.first("crm.Average Complaint Resolution Time (mins)").alias("Average Complaint Resolution Time (mins)")
        )
        #append to consumer_dim
        time.sleep(1)
        start = time.time()
        # consumer_ageing_crm_dim.writeTo("nessie.starschema.consumer_ageing_crm_dim").append()
        # consumer_ageing_crm_dim.limit(5).show(truncate=False)      
        timetaken = time.time()-starttime
        logging.info("Time taken to join with CRM Consumer dim and ingesting: %.2f seconds", timetaken)
        timedict["Joining with CRM Consumer dim and ingesting"] = timetaken
    #----------------------- ageing x crm IBC join
        logging.info("Appending Ageing x CRM IBC dim - 33 IBCs")
        ibc_crm_dim_ = spark.read.table("nessie.starschema.ibc_crm_dim")
        ibc_crm_ageing_dim_33 = ageing_new_data.alias("ageing").join(
            ibc_crm_dim_.alias("ibc_crm"), 
            (F.col("ageing.IBC") == F.col("ibc_crm.IBCCode")) &
            (F.col("ageing.Month") == F.col("ibc_crm.Month")) &
            (F.col("ageing.Year") == F.col("ibc_crm.Year")),
            how='inner'
        ).groupBy("ageing.IBC", "ageing.Month", "ageing.Year").agg(
            # First value of relevant Ageing columns (from ageing_new_data)
            F.first("ageing.IBCName").alias("IBC Name"),
            F.first("ageing.Region").alias("Region"),
            F.first("ageing.BillingMonth").alias("Billing Month"),
            
            # Aggregated values from CRM
            F.first("ibc_crm.Complaint Counts").alias("Complaint Counts"),
            F.first("ibc_crm.Average Complaint Resolution Time (mins)").alias("Average Complaint Resolution Time (mins)"),
            # First value of relevant Ageing columns (from ageing_new_data)            
            F.sum("ageing.CurrentAmount").alias("Total Current Amount"),
            F.avg("ageing.CurrentAmount").alias("Average Current Amount"),
            F.sum("ageing.CurrentUnits").alias("Total Current Units"),
            F.avg("ageing.CurrentUnits").alias("Average Current Units"),
        
            F.sum("ageing.Sanctioned Load").alias("Total Sanctioned Load"),
            F.avg("ageing.Sanctioned Load").alias("Average Sanctioned Load"),
            F.sum("ageing.Connected Load").alias("Total Connected Load"),
            F.avg("ageing.Connected Load").alias("Average Connected Load"),
            
            F.sum("ageing.IRBAmount").alias("Total IRB Amount"),
            F.avg("ageing.IRBAmount").alias("Average IRB Amount"),
            F.sum("ageing.IRBUnits").alias("Total IRB_Units"),
            F.avg("ageing.IRBUnits").alias("Average IRB Units")
        )
        time.sleep(1)
        start = time.time()
        # ibc_crm_ageing_dim_33.writeTo("nessie.starschema.ibc_crm_ageing_dim_33").append()
        ibc_crm_ageing_dim_33.limit(5).show(truncate=False)
        timetaken = time.time()-starttime
        logging.info("Time taken to join with CRM IBC dim - 33 IBCs - and ingesting: %.2f seconds", timetaken)
        timedict["Joining with CRM IBC dim and ingesting"] = timetaken
    #----------------------- crm oms ibc        
        logging.info("Appending Ageing x CRM x OMS IBC dim - 4 IBCs")
        ibc_oms_crm_dim = spark.read.table("nessie.starschema.ibc_oms_crm_dim")
        ibc_all_dim_4 = ageing_new_data.alias("ageing").join(
            ibc_oms_crm_dim.alias("ibc_crm_oms"), 
            (F.col("ageing.IBC") == F.col("ibc_crm_oms.IBCCode")) &
            (F.col("ageing.Month") == F.col("ibc_crm_oms.Month")) &
            (F.col("ageing.Year") == F.col("ibc_crm_oms.Year")),
            how='inner'
        ).groupBy("ageing.IBC", "ageing.Month", "ageing.Year").agg(
            # First value of relevant Ageing columns (from ageing_new_data)
            F.first("ageing.IBCName").alias("IBC Name"),
            F.first("ageing.Region").alias("Region"),
            F.first("ageing.BillingMonth").alias("Billing Month"),
            # Aggregations are already calculated in oms crm dim
            F.first("ibc_crm_oms.Complaint Counts").alias("Complaint Counts"),
            F.first("ibc_crm_oms.Average Complaint Resolution Time (mins)").alias("Average Complaint Resolution Time (mins)"),
            F.first("ibc_crm_oms.Total Outages/Faults").alias("Total Outages/Faults"),
            F.first("ibc_crm_oms.Most_Occurred_Fault").alias("Most Occurred Fault"),
            F.first("ibc_crm_oms.Most_Occurred_Fault_Frequency").alias("Most Occurred Fault Frequency"),
            F.first("ibc_crm_oms.Average Fault Turn-Around Time (mins)").alias("Average Fault Turn-Around Time (mins)"),
            F.first("ibc_crm_oms.Average Fault Duration (mins)").alias("Average Fault Duration (mins)"),
            F.first("ibc_crm_oms.Rain Frequency").alias("Rain Frequency"),
        
            # Aggregations from Ageing Data (IRB related columns)            
            F.sum("ageing.CurrentAmount").alias("Total Current Amount"),
            F.avg("ageing.CurrentAmount").alias("Average Current Amount"),
            F.sum("ageing.CurrentUnits").alias("Total Current Units"),
            F.avg("ageing.CurrentUnits").alias("Average Current Units"),
        
            F.sum("ageing.Sanctioned Load").alias("Total Sanctioned Load"),
            F.avg("ageing.Sanctioned Load").alias("Average Sanctioned Load"),
            F.sum("ageing.Connected Load").alias("Total Connected Load"),
            F.avg("ageing.Connected Load").alias("Average Connected Load"),
            
            F.sum("ageing.IRBAmount").alias("Total IRB Amount"),
            F.avg("ageing.IRBAmount").alias("Average IRB Amount"),
            F.sum("ageing.IRBUnits").alias("Total IRB_Units"),
            F.avg("ageing.IRBUnits").alias("Average IRB Units"),
        )
        time.sleep(1)
        start = time.time()
        # ibc_all_dim_4.limit(5).show(truncate=False)
        # ibc_all_dim_4.writeTo("nessie.starschema.ibc_all_dim_4").append()
        timetaken = time.time()-start
        logging.info("Time taken to join with CRM x OMS IBC dim - 4 IBCs: {%.2f seconds", timetaken)
        timedict["Joining with CRM and OMS IBC dim - 4 IBCs and ingesting"] = timetaken
        #----------------------- crm pmt        
        logging.info("Appending Ageing x CRM PMT IBC dim")
        pmt_crm_dim = spark.read.table("nessie.starschema.pmt_crm_dim")
        pmt_crm_ageing_dim = ageing_new_data.alias("ageing").join(
            pmt_crm_dim.alias("pmt_crm"), 
            (F.col("ageing.PMT") == F.col("pmt_crm.PMT")) &
            (F.col("ageing.Month") == F.col("pmt_crm.Month")) &
            (F.col("ageing.Year") == F.col("pmt_crm.Year")),
            how='inner'
        ).groupBy("ageing.PMT", "ageing.Month", "ageing.Year").agg(
            # First value of relevant Ageing columns (from ageing_new_data)
            F.first("pmt_crm.PMT Name").alias("PMT Name"),    
            F.first("ageing.IBC").alias("IBC Code"),
            F.first("ageing.IBCName").alias("IBC Name"),
            F.first("ageing.Region").alias("Region"),
            F.first("ageing.BillingMonth").alias("Billing Month"),
            # Aggregations are already calculated in oms crm dim
            F.first("pmt_crm.Complaint Counts").alias("Complaint Counts"),
            F.first("pmt_crm.Average Complaint Resolution Time (mins)").alias("Average Complaint Resolution Time (mins)"),
            # Aggregations from Ageing Data
            F.sum("ageing.CurrentAmount").alias("Total Current Amount"),
            F.avg("ageing.CurrentAmount").alias("Average Current Amount"),
            F.sum("ageing.CurrentUnits").alias("Total Current Units"),
            F.avg("ageing.CurrentUnits").alias("Average Current Units"),
            F.sum("ageing.Sanctioned Load").alias("Total Sanctioned Load"),
            F.avg("ageing.Sanctioned Load").alias("Average Sanctioned Load"),
            F.sum("ageing.Connected Load").alias("Total Connected Load"),
            F.avg("ageing.Connected Load").alias("Average Connected Load"),            
            F.sum("ageing.IRBAmount").alias("Total IRB Amount"),
            F.avg("ageing.IRBAmount").alias("Average IRB Amount"),
            F.sum("ageing.IRBUnits").alias("Total IRB_Units"),
            F.avg("ageing.IRBUnits").alias("Average IRB Units"))
        time.sleep(1)
        start = time.time()
        pmt_crm_ageing_dim.limit(5).show(truncate=False)
        # pmt_crm_ageing_dim.writeTo("nessie.starschema.pmt_crm_ageing_dim").append()
        timetaken = time.time()-start
        logging.info("Time taken to join with CRM PMT IBC dim: %.2f seconds", timetaken)
        timedict["Joining with CRM PMT IBC dim and ingesting"] = timetaken
        logging.info("New data has been joined with CRM and ingested")
    else:
        logging.info("Billing month does not have corresponding data in CRM from Apr-Sep")

    #----------------------- crm ingestion completed
    #----------------------- oms ageing ibc
    logging.info("Appending OMS x Ageing on IBC - 4 IBCs")
    ibc_oms_dim = spark.read.table("nessie.starschema.ibc_oms_dim")    
    ibc_oms_ageing_dim_4 = ageing_new_data.alias("ageing").join(
        ibc_oms_dim.alias("ibc_oms"), 
        (F.col("ageing.IBCName") == F.col("ibc_oms.IBC")) & #oms does not have ibc codes
        (F.col("ageing.Month") == F.col("ibc_oms.Month")) &
        (F.col("ageing.Year") == F.col("ibc_oms.Year")),
        how='inner'
    ).groupBy("ageing.IBC", "ageing.Month", "ageing.Year").agg(
        # First value of relevant Ageing columns (from ageing_new_data)
        F.first("ageing.IBCName").alias("IBC Name"),
        F.first("ageing.Region").alias("Region"),
        F.first("ageing.BillingMonth").alias("Billing Month"),
        # Aggregations are already calculated in oms ibc dim
        F.first("ibc_oms.Total Outages/Faults").alias("Total Outages/Faults"),
        F.first("ibc_oms.Most_Occurred_Fault").alias("Most Occurred Fault"),
        F.first("ibc_oms.Most_Occurred_Fault_Frequency").alias("Most Occurred Fault Frequency"),
        F.first("ibc_oms.Average Fault Turn-Around Time (mins)").alias("Average Fault Turn-Around Time (mins)"),
        F.first("ibc_oms.Average Fault Duration (mins)").alias("Average Fault Duration (mins)"),
        F.first("ibc_oms.Rain Frequency").alias("Rain Frequency"),
        # Aggregations from Ageing Data        
        F.sum("ageing.CurrentAmount").alias("Total Current Amount"),
        F.avg("ageing.CurrentAmount").alias("Average Current Amount"),
        F.sum("ageing.CurrentUnits").alias("Total Current Units"),
        F.avg("ageing.CurrentUnits").alias("Average Current Units"),
    
        F.sum("ageing.Sanctioned Load").alias("Total Sanctioned Load"),
        F.avg("ageing.Sanctioned Load").alias("Average Sanctioned Load"),
        F.sum("ageing.Connected Load").alias("Total Connected Load"),
        F.avg("ageing.Connected Load").alias("Average Connected Load"),
        
        F.sum("ageing.IRBAmount").alias("Total IRB Amount"),
        F.avg("ageing.IRBAmount").alias("Average IRB Amount"),
        F.sum("ageing.IRBUnits").alias("Total IRB_Units"),
        F.avg("ageing.IRBUnits").alias("Average IRB Units"),
    )
    time.sleep(1)
    start = time.time()
    ibc_oms_ageing_dim_4.limit(5).show(truncate=False)
    # ibc_oms_ageing_dim_4.writeTo("nessie.starschema.ibc_oms_ageing_dim_4").append()
    timetaken = time.time()-start
    logging.info("Time taken to join with OMS IBC and ingesting: %.2f seconds", timetaken)
    timedict["Joining with OMS IBC and ingesting"] = timetaken
    #----------------------- DONE
    logging.info("All transformations are completed!")
    
    spark.stop()
    logging.info("Spark Session Stopped")
    
    # Log timing breakdown
    total_time = 0
    logging.info("----- Timing Breakdown -----")
    for step, duration in timedict.items():
        logging.info(f"{step}: {duration:.2f} seconds")
        total_time+=duration
    logging.info(f"Total time taken: {total_time:.2f} seconds")
    # ---------------------------------------
    
    logging.info("[INFO] Incremental load script completed.")

    # Create done.signal in the new directory
    signal_file = "/mnt/inc_processed/done.signal"
    if not os.path.exists(signal_file):
        with open(signal_file, "w") as f:
            f.write("DONE")
        logging.info("[INFO] done.signal created.")
    else:
        logging.info("[INFO] done.signal already exists. Skipping creation.")

    
if __name__ == "__main__":
    main()
