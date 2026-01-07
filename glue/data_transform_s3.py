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

from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

BRONZE_PATH = "s3://kavyabd/rawbronze/rawtoprocess/"
SILVER_PATH = "s3://kavyabd/silverstaging/"
DLQ_PATH = "s3://kavyabd/dlq/silver-violations/"
PYDEEQU_RESULTS_PATH = "s3://kavyabd/data-quality-results/pydeequ/"

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
    dlq_output = f"{DLQ_PATH}schema_violations_{timestamp_str}.json"
    invalid_df.coalesce(1).write.mode("append").json(dlq_output)
    print(f"SCHEMA ENFORCEMENT: {len(invalid_records)} records sent to DLQ: {dlq_output}")

if not valid_records:
    print("No valid records to process after schema enforcement")
    job.commit()
    sys.exit(0)

validated_df = spark.createDataFrame(valid_records)
print(f"SCHEMA ENFORCEMENT: {validated_df.count()} valid records proceeding")

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

window_spec = Window.partitionBy("coin_id", "update_date").orderBy(F.col("last_updated_ts").desc())
deduped = transformed.withColumn("row_num", F.row_number().over(window_spec)) \
                     .filter(F.col("row_num") == 1) \
                     .drop("row_num")

print(f"After deduplication: {deduped.count()} records")

print("PYDEEQU DATA QUALITY VALIDATION STARTING")

analysisResult = AnalysisRunner(spark) \
    .onData(deduped) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("coin_id")) \
    .addAnalyzer(Completeness("symbol")) \
    .addAnalyzer(Completeness("name")) \
    .addAnalyzer(Completeness("current_price")) \
    .addAnalyzer(Completeness("market_cap")) \
    .addAnalyzer(Uniqueness("coin_id")) \
    .addAnalyzer(Mean("current_price")) \
    .addAnalyzer(Mean("market_cap")) \
    .addAnalyzer(StandardDeviation("current_price")) \
    .addAnalyzer(Minimum("current_price")) \
    .addAnalyzer(Maximum("current_price")) \
    .addAnalyzer(ApproxCountDistinct("coin_id")) \
    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
metrics_output = f"{PYDEEQU_RESULTS_PATH}metrics/run_{timestamp_str}.parquet"
analysisResult_df.coalesce(1).write.mode("overwrite").parquet(metrics_output)
print(f"PyDeequ metrics saved: {metrics_output}")

analysisResult_df.show(truncate=False)

check = Check(spark, CheckLevel.Error, "Data Quality Checks")

check.hasSize(lambda x: x >= 50) \
     .hasSize(lambda x: x <= 150) \
     .isComplete("coin_id") \
     .isComplete("symbol") \
     .isComplete("name") \
     .isComplete("current_price") \
     .isComplete("market_cap") \
     .isUnique("coin_id") \
     .hasMin("current_price", lambda x: x > 0.0) \
     .hasMax("current_price", lambda x: x < 1000000.0) \
     .hasMin("market_cap", lambda x: x > 0) \
     .isContainedIn("market_cap_rank", [str(i) for i in range(1, 10001)])

verificationResult = VerificationSuite(spark) \
    .onData(deduped) \
    .addCheck(check) \
    .run()

verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
verification_output = f"{PYDEEQU_RESULTS_PATH}verification/run_{timestamp_str}.parquet"
verificationResult_df.coalesce(1).write.mode("overwrite").parquet(verification_output)
print(f"PyDeequ verification results saved: {verification_output}")

verificationResult_df.show(truncate=False)

if verificationResult.status == "Success":
    print("PYDEEQU VALIDATION: All checks passed")
else:
    print("PYDEEQU VALIDATION: Some checks failed")
    failed_checks = verificationResult_df.filter(F.col("check_status") == "Error")
    if failed_checks.count() > 0:
        failed_checks.show(truncate=False)
        
        failed_records_output = f"{DLQ_PATH}quality_violations_{timestamp_str}.json"
        failed_info = failed_checks.select(
            F.lit(datetime.now().isoformat()).alias("timestamp"),
            F.col("check"),
            F.col("check_status"),
            F.col("constraint"),
            F.col("constraint_status"),
            F.col("constraint_message")
        )
        failed_info.coalesce(1).write.mode("append").json(failed_records_output)
        print(f"Quality violations logged to DLQ: {failed_records_output}")

(
    deduped.write
        .mode("append")
        .format("parquet")
        .partitionBy("update_date")
        .option("compression", "snappy")
        .save(SILVER_PATH)
)

print(f"Successfully appended {deduped.count()} records to Silver layer")

job.commit()
