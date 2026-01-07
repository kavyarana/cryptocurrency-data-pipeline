import sys
import json
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import ResolveChoice
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

BRONZE_PATH = "s3://kavyabd/rawbronze/rawtoprocess/"
SILVER_INTERMEDIATE_PATH = "s3://kavyabd/silver-intermediate/"
DLQ_SCHEMA_PATH = "s3://kavyabd/dlq/silver-violations/schema/"

EXPECTED_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("name", StringType(), False),
    StructField("current_price", DoubleType(), False),
    StructField("market_cap", LongType(), False),
    StructField("market_cap_rank", IntegerType(), True),
    StructField("total_volume", LongType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("circulating_supply", DoubleType(), True),
    StructField("total_supply", DoubleType(), True)
])

print("TRANSFORM JOB: SCHEMA ENFORCEMENT AND BASIC TRANSFORMATION")

datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [BRONZE_PATH],
        "recurse": True
    },
    transformation_ctx="datasource"
)

resolved = ResolveChoice.apply(
    frame=datasource,
    choice="cast:double",
    transformation_ctx="resolved"
)

df = resolved.toDF()

if df.count() == 0:
    print("No new data to process")
    job.commit()
    sys.exit(0)

print(f"Processing {df.count()} records from Bronze")

print("STEP 1: SCHEMA ENFORCEMENT")

valid_records = []
invalid_records = []

for row in df.collect():
    row_dict = row.asDict()
    is_valid = True
    missing_fields = []
    
    for field in EXPECTED_SCHEMA.fields:
        if field.name not in row_dict:
            if not field.nullable:
                is_valid = False
                missing_fields.append(field.name)
    
    if is_valid:
        valid_records.append(row_dict)
    else:
        invalid_records.append({
            "raw_data": json.dumps(row_dict),
            "error_reason": f"Missing required fields: {', '.join(missing_fields)}",
            "timestamp": datetime.now().isoformat(),
            "validation_type": "schema_enforcement"
        })

if invalid_records:
    invalid_df = spark.createDataFrame(invalid_records)
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    dlq_output = f"{DLQ_SCHEMA_PATH}violations_{timestamp_str}.json"
    invalid_df.coalesce(1).write.mode("append").json(dlq_output)
    print(f"SCHEMA ENFORCEMENT: {len(invalid_records)} records sent to DLQ")

if not valid_records:
    print("No valid records after schema enforcement")
    job.commit()
    sys.exit(0)

validated_df = spark.createDataFrame(valid_records)
print(f"SCHEMA ENFORCEMENT: {validated_df.count()} valid records proceeding")

print("STEP 2: DATA TRANSFORMATION")

transformed = validated_df.select(
    F.col("id").alias("coin_id"),
    F.col("symbol").cast("string"),
    F.col("name").cast("string"),
    F.col("current_price").cast("double"),
    F.col("market_cap").cast("long"),
    F.col("market_cap_rank").cast("int"),
    F.col("total_volume").cast("long"),
    F.col("high_24h").cast("double"),
    F.col("low_24h").cast("double"),
    F.col("price_change_24h").cast("double"),
    F.col("price_change_percentage_24h").cast("double"),
    F.col("circulating_supply").cast("double"),
    F.col("total_supply").cast("double"),
    F.current_date().alias("update_date"),
    F.current_timestamp().alias("last_updated_ts")
)

print("STEP 3: DEDUPLICATION")

window_spec = Window.partitionBy("coin_id", "update_date").orderBy(F.col("last_updated_ts").desc())
deduped = transformed.withColumn("row_num", F.row_number().over(window_spec)) \
                     .filter(F.col("row_num") == 1) \
                     .drop("row_num")

print(f"After deduplication: {deduped.count()} records")

print("STEP 4: WRITING TO INTERMEDIATE LOCATION")

(
    deduped.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(SILVER_INTERMEDIATE_PATH)
)

print(f"Transform complete: {deduped.count()} records written to intermediate location")

job.commit()
