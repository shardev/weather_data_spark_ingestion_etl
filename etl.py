import pyspark
import pyspark.sql
from delta import *
from delta.tables import DeltaTable
from pyspark.sql.functions import col, datediff, avg, date_format, to_timestamp
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, FloatType, DateType, IntegerType, LongType
import json, datetime, os, sys

# ------------------------------
# Project config
# ------------------------------
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

builder = pyspark.sql.SparkSession.builder.appName("WeatherIngestionETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Configurable param to set appropriate environment: dev/prod
env = "dev"  
date_param = '2021-04-01' if env == "dev" else datetime.today().strftime('%Y-%m-%d') 

# Read config for the specific environment
with open("config.json") as f:
    config = json.load(f)

# ------------------------------
# Raw layer
# ------------------------------
raw_path = config[env]['raw_data_path']

# Read device CSV data
device_path = config[env]['device_data_source_path']
device_data_df = spark.read.option("header", "true").csv(device_path)
if env == 'dev':
    device_data_df.printSchema() 
    device_data_df.show()

# Define the schema for the raw layer
raw_device_data_schema = StructType([
    StructField("code", StringType(), False),
    StructField("type", StringType(), True),
    StructField("area", StringType(), True),
    StructField("customer", StringType(), True)
])
 
# Apply schema enforcement using the defined schema on DF creation
device_data_df_wschema = spark.createDataFrame(data = device_data_df.rdd, schema = raw_device_data_schema)

# Writing data to Delta Lake
print("Ingestion for [Raw layer]: device_data started.")
device_data_df_wschema.write.format("delta").mode("overwrite").save(raw_path + "/device_data")
print("Ingestion for [Raw layer]: device_data completed.")

# Read daily sensor JSON data from the constructed path
sensor_path = config[env]['sensor_data_source_path']
sensor_data_df = spark.read.option("basePath", sensor_path).json(sensor_path + f"/received={date_param}")
if env == 'dev':
    sensor_data_df.printSchema()
    sensor_data_df.show()

# Schema enforcement for raw layer with source data types for DF creation
raw_sensor_data_schema = StructType([
    StructField("CO2_level", IntegerType(), True),
    StructField("device", StringType(), False),
    StructField("humidity", IntegerType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("timestamp", StringType(), False),
    StructField("received", DateType(), True)
])

# Writing data to Delta Lake
print("Ingestion for [Raw layer]: sensor_data started.")
sensor_data_df_wschema = spark.createDataFrame(data = sensor_data_df.rdd, schema = raw_sensor_data_schema)
sensor_data_df_wschema.write.format("delta").mode("append").save(raw_path + "/sensor_data")
print("Ingestion for [Raw layer]: device_data completed.")

# ------------------------------
# Processing layer
# ------------------------------
processed_path = config[env]['processed_data_path']

# Get only data for current daily run from raw layer - not to update existing historical data
sensor_data_w_late_arrivals = spark.read.format("delta").load(raw_path + "/sensor_data")
sensor_data_w_late_arrivals = sensor_data_w_late_arrivals.where(f"received = '{date_param}'")

# Show examples with duplicates
if env == 'dev':
    duplicate_data_df = (
        sensor_data_w_late_arrivals \
        .groupby(['device', 'timestamp']) \
        .count() \
        .where('count > 1') 
    )
    duplicate_data_df.show()

# Delete duplicates
# Samples that are recorded by the same `device` at the same `timestamp` are considered duplicates.
print('[Pre deletion] duplicates: ' + str(sensor_data_w_late_arrivals.count()))
deduplicated_df = sensor_data_w_late_arrivals.drop_duplicates(["device", "timestamp"])

# Schema enforcement for processing layer with appropriate data types for DF creation
processed_sensor_data_schema = StructType([
    StructField("CO2_level", LongType(), True),
    StructField("device", StringType(), False),
    StructField("humidity", LongType(), True),
    StructField("temperature", LongType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("received", DateType(), True)
])

deduplicated_df = deduplicated_df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

deduplicated_df_wshema = spark.createDataFrame(data = deduplicated_df.rdd, schema = processed_sensor_data_schema)
deduplicated_df_wshema.write.format("delta").mode("append").save(processed_path + "/sensor_data")
print('[Post deletion] duplicates: ' + str(deduplicated_df.count()))

# Delete late arrivals
sensor_data_processed = DeltaTable.forPath(spark, processed_path + "/sensor_data")
print('[Pre deletion] late arrivals: ' + str(sensor_data_processed.toDF().count()))

# Show examples with late arrivals
if env == 'dev':
    late_arrival_df = (
        sensor_data_processed.toDF() \
        .where(datediff(col("received"), col("timestamp")) > 1)
    )
    late_arrival_df.show()

# Execute the delete operation on Delta table
sensor_data_processed.delete(condition = datediff(col("received"), col("timestamp")) > 1)
print('[Post deletion] late arrivals: ' + str(sensor_data_processed.toDF().count()))


# ------------------------------
# Report layer
# ------------------------------
report_path = config[env]['report_data_path']

# Load processed sensor data and device info
device_data = spark.read.format("delta").load(raw_path + "/device_data")
sensor_data = spark.read.format("delta").load(processed_path + "/sensor_data")

# Join sensor data with device info to get area type
sensor_data = sensor_data.withColumnRenamed("device", "code")
sensor_with_area = sensor_data.join(
    device_data,
    on="code", 
    how="inner" 
).select(
    "code", "area", "timestamp", "CO2_level", "humidity", "temperature"
)

# Extract month from the timestamp
sensor_with_area = sensor_with_area.withColumn("month", date_format("timestamp", "M").cast("int"))

# Compute the monthly averages per area type
report_data = sensor_with_area.groupBy("month", "area") \
    .agg(
        avg("CO2_level").alias("avg_CO2_level"),
        avg("humidity").alias("avg_humidity"),
        avg("temperature").alias("avg_temperature")
    )

# Schema enforcement for processing layer with appropriate data types
report_sensor_data_schema = StructType([
    StructField("month", IntegerType(), False),
    StructField("area", StringType(), False),
    StructField("avg_CO2_level", FloatType(), True),
    StructField("avg_humidity", FloatType(), True),
    StructField("avg_temperature", FloatType(), True)
])
report_data_wschema = spark.createDataFrame(data = report_data.rdd, schema = report_sensor_data_schema)

# Overwrite existing report because daily new data for existing months is genereted and it will affect calculations
report_data_wschema.write.format("delta").mode("overwrite").save(report_path + "/monthly_report")

if env == 'dev':
    report_data.show()