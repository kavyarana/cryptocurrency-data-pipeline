import sys
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

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

SILVER_INTERMEDIATE_PATH = "s3://kavyabd/silver-intermediate/"
SILVER_PYDEEQU_GOOD_PATH = "s3://kavyabd/silver-pydeequ-validated/"
DLQ_PYDEEQU_PATH = "s3://kavyabd/dlq/silver-violations/pydeequ/"
PYDEEQU_RESULTS_PATH = "s3://kavyabd/data-quality-results/pydeequ/"

print("PYDEEQU DATA QUALITY JOB STARTED")

df = spark.read.parquet(SILVER_INTERMEDIATE_PATH)

if df.count() == 0:
    print("No data to validate")
    job.commit()
    sys.exit(0)

print(f"Validating {df.count()} records with PyDeequ")

print("STEP 1: PYDEEQU ANALYSIS")

analysisResult = AnalysisRunner(spark) \
    .onData(df) \
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
    .addAnalyzer(Minimum("market_cap")) \
    .addAnalyzer(Maximum("market_cap")) \
    .addAnalyzer(ApproxCountDistinct("coin_id")) \
    .addAnalyzer(ApproxQuantile("current_price", 0.5)) \
    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
metrics_output = f"{PYDEEQU_RESULTS_PATH}metrics/run_{timestamp_str}.parquet"
analysisResult_df.coalesce(1).write.mode("overwrite").parquet(metrics_output)
print(f"PyDeequ metrics saved: {metrics_output}")

analysisResult_df.show(truncate=False)

print("STEP 2: PYDEEQU VERIFICATION")

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
     .hasMin("market_cap", lambda x: x > 0)

verificationResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check) \
    .run()

verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
verification_output = f"{PYDEEQU_RESULTS_PATH}verification/run_{timestamp_str}.parquet"
verificationResult_df.coalesce(1).write.mode("overwrite").parquet(verification_output)
print(f"PyDeequ verification results saved: {verification_output}")

verificationResult_df.show(truncate=False)

print("STEP 3: HANDLING VALIDATION RESULTS")

if verificationResult.status == "Success":
    print("PYDEEQU: All checks passed")
    
    (
        df.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .save(SILVER_PYDEEQU_GOOD_PATH)
    )
    
    print(f"All {df.count()} records passed PyDeequ validation")
    
else:
    print("PYDEEQU: Some checks failed")
    
    failed_checks = verificationResult_df.filter(
        (F.col("check_status") == "Error") | 
        (F.col("constraint_status") == "Failure")
    )
    
    if failed_checks.count() > 0:
        failed_checks.show(truncate=False)
        
        failed_output = f"{DLQ_PYDEEQU_PATH}violations_{timestamp_str}.json"
        failed_checks.coalesce(1).write.mode("append").json(failed_output)
        print(f"Failed checks logged to: {failed_output}")
    
    (
        df.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .save(SILVER_PYDEEQU_GOOD_PATH)
    )
    
    print(f"Data written despite failures for downstream processing")

job.commit()
