import sys
import re
import concurrent.futures
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import Filter, SelectFromCollection
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality

class GroupFilter:
    def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return Filter.apply(frame=source_DyF, f=group.filters)

def threadedRoute(glue_ctx, source_DyF, group_filters):
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {
            executor.submit(apply_group_filter, source_DyF, gf): gf
            for gf in group_filters
        }
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

def drop_dq_array_fields(dyf):
    return dyf.drop_fields([
        "DataQualityRulesSkip",
        "DataQualityRulesPass",
        "DataQualityRulesFail",
        "DataQualityEvaluationResult"
    ])

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PYDEEQU_GOOD_PATH = "s3://kavyabd/silver-pydeequ-validated/"
SILVER_FINAL_PATH = "s3://kavyabd/silverstaging/"
DLQ_DQDL_PATH = "s3://kavyabd/dlq/silver-violations/dqdl/"

print("DQDL DATA QUALITY JOB STARTED")

df = spark.read.parquet(SILVER_PYDEEQU_GOOD_PATH)

if df.count() == 0:
    print("No data to validate")
    job.commit()
    sys.exit(0)

print(f"Validating {df.count()} records with DQDL")

source_dyf = DynamicFrame.fromDF(df, glueContext, "source_dyf")

print("STEP 1: DQDL EVALUATION")

dqdl_ruleset = """
Rules = [
    ColumnExists "coin_id",
    ColumnExists "current_price",
    ColumnExists "market_cap",
    ColumnExists "symbol",
    ColumnExists "name",
    IsComplete "coin_id",
    IsComplete "symbol",
    IsComplete "name",
    IsComplete "current_price",
    IsComplete "market_cap",
    IsPrimaryKey "coin_id",
    ColumnValues "current_price" > 0,
    ColumnValues "market_cap" > 0,
    RowCount between 50 and 150,
    Completeness "current_price" > 0.95,
    Completeness "market_cap" > 0.95,
    Uniqueness "coin_id" > 0.99
]
"""

dqdl_results = EvaluateDataQuality().process_rows(
    frame=source_dyf,
    ruleset=dqdl_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "crypto_silver_dqdl",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "observations.scope": "ALL",
        "performanceTuning.caching": "LAZY"
    }
)

row_level_outcomes = SelectFromCollection.apply(
    dfc=dqdl_results,
    key="rowLevelOutcomes"
)

print("STEP 2: ROUTING GOOD AND BAD RECORDS")

routed_frames = threadedRoute(
    glueContext,
    source_DyF=row_level_outcomes,
    group_filters=[
        GroupFilter(
            name="dqdl_passed",
            filters=lambda r: bool(re.match("Passed", r["DataQualityEvaluationResult"]))
        ),
        GroupFilter(
            name="dqdl_failed",
            filters=lambda r: bool(re.match("Failed", r["DataQualityEvaluationResult"]))
        )
    ]
)

dqdl_passed = SelectFromCollection.apply(dfc=routed_frames, key="dqdl_passed")
dqdl_failed = SelectFromCollection.apply(dfc=routed_frames, key="dqdl_failed")

dqdl_passed_clean = drop_dq_array_fields(dqdl_passed)
dqdl_failed_clean = drop_dq_array_fields(dqdl_failed)

passed_count = dqdl_passed_clean.toDF().count()
failed_count = dqdl_failed_clean.toDF().count()

print(f"DQDL Results: {passed_count} passed, {failed_count} failed")

print("STEP 3: WRITING RESULTS")

if failed_count > 0:
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    glueContext.write_dynamic_frame.from_options(
        frame=dqdl_failed_clean,
        connection_type="s3",
        format="json",
        connection_options={
            "path": f"{DLQ_DQDL_PATH}violations_{timestamp_str}/"
        }
    )
    print(f"Failed records written to DLQ: {failed_count} records")

if passed_count > 0:
    final_df = dqdl_passed_clean.toDF()
    
    (
        final_df.write
            .mode("append")
            .format("parquet")
            .partitionBy("update_date")
            .option("compression", "snappy")
            .save(SILVER_FINAL_PATH)
    )
    
    print(f"Successfully written {passed_count} records to Silver layer")
else:
    print("No records passed DQDL validation")

job.commit()
