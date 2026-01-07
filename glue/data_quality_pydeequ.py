import sys
import os

os.environ['SPARK_VERSION'] = '3.3'

from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from pydeequ.analyzers import AnalysisRunner, AnalyzerContext
from pydeequ.analyzers import Size, Completeness, Uniqueness, Mean, StandardDeviation, Minimum, Maximum, ApproxCountDistinct
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_INTERMEDIATE_PATH = "s3://kavyabd/silver-intermediate/"
SILVER_VALIDATED_PATH = "s3://kavyabd/silver-pydeequ-validated/"
DLQ_PATH = "s3://kavyabd/dlq/silver-violations/pydeequ/"
RESULTS_PATH = "s3://kavyabd/data-quality-results/pydeequ/"

print("PYDEEQU DATA QUALITY JOB STARTED - Glue 4.0 / Spark 3.3")

df = spark.read.parquet(SILVER_INTERMEDIATE_PATH)

record_count = df.count()
print(f"Read {record_count} records from Silver intermediate")

if record_count == 0:
    raise Exception("No data found in silver-intermediate layer")

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
    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
metrics_output = f"{RESULTS_PATH}metrics/run_{timestamp_str}.parquet"
analysisResult_df.coalesce(1).write.mode("overwrite").parquet(metrics_output)
print(f"PyDeequ metrics saved: {metrics_output}")

print("Analysis Metrics:")
analysisResult_df.show(truncate=False)

print("STEP 2: PYDEEQU VERIFICATION")

check = Check(spark, CheckLevel.Error, "Data Quality Checks")

check = check.hasSize(lambda x: x >= 50, "Record count should be at least 50")
check = check.hasSize(lambda x: x <= 150, "Record count should not exceed 150")
check = check.isComplete("coin_id", "coin_id must be complete")
check = check.isComplete("symbol", "symbol must be complete")
check = check.isComplete("name", "name must be complete")
check = check.isComplete("current_price", "current_price must be complete")
check = check.isComplete("market_cap", "market_cap must be complete")
check = check.isUnique("coin_id", "coin_id must be unique")
check = check.hasMin("current_price", lambda x: x > 0.0, "current_price must be positive")
check = check.hasMax("current_price", lambda x: x < 1000000.0, "current_price must be reasonable")
check = check.hasMin("market_cap", lambda x: x > 0, "market_cap must be positive")

verificationResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check) \
    .run()

verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
verification_output = f"{RESULTS_PATH}verification/run_{timestamp_str}.parquet"
verificationResult_df.coalesce(1).write.mode("overwrite").parquet(verification_output)
print(f"PyDeequ verification results saved: {verification_output}")

print("Verification Results:")
verificationResult_df.show(truncate=False)

print("STEP 3: CHECK VALIDATION STATUS")

if verificationResult.status == "Success":
    print("PYDEEQU VALIDATION PASSED: All checks successful")
    
    (
        df.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .save(SILVER_VALIDATED_PATH)
    )
    
    print(f"Successfully validated and wrote {df.count()} records")
    
else:
    print("PYDEEQU VALIDATION FAILED: Some checks did not pass")
    
    failed_checks = verificationResult_df.filter(
        (F.col("check_status") == "Error") | 
        (F.col("constraint_status") == "Failure")
    )
    
    failed_count = failed_checks.count()
    
    if failed_count > 0:
        print(f"Number of failed checks: {failed_count}")
        print("Failed checks details:")
        failed_checks.show(truncate=False)
        
        failed_output = f"{DLQ_PATH}violations_{timestamp_str}.json"
        failed_checks.coalesce(1).write.mode("append").json(failed_output)
        print(f"Failed validation details saved to: {failed_output}")
    
    raise Exception(f"PyDeequ validation FAILED. {failed_count} checks did not pass. Check logs and DLQ for details.")

job.commit()
