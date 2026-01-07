import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import ResolveChoice
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

BRONZE_PATH = "s3://kavyabd/rawbronze/rawtoprocess/"
SILVER_PATH = "s3://kavyabd/silverstaging/"

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
    print("No new data to process. Exiting.")
    job.commit()
    sys.exit(0)

print(f"Processing {df.count()} new records from Bronze")

transformed = df.select(
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

(
    deduped.write
        .mode("append")
        .format("parquet")
        .partitionBy("update_date")
        .option("compression", "snappy")
        .save(SILVER_PATH)
)

print(f"Appended {deduped.count()} records to Silver layer")

job.commit()