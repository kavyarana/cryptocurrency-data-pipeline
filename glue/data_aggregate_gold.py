import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

SILVER_PATH = "s3://kavyabd/silverstaging/"
GOLD_PATH = "s3://kavyabd/goldlayer/"

df_silver = spark.read.parquet(SILVER_PATH)

today = F.current_date()

df_new = df_silver.filter(F.col("update_date") == today)

if df_new.count() == 0:
    print("No new data for today. Exiting.")
    job.commit()
    sys.exit(0)

print(f"Processing {df_new.count()} records for today")

fact_new = df_new.select(
    F.col("coin_id"),
    F.col("update_date").alias("date"),
    F.col("symbol"),
    F.col("name"),
    F.col("current_price"),
    F.col("market_cap"),
    F.col("market_cap_rank"),
    F.col("total_volume"),
    F.col("high_24h"),
    F.col("low_24h"),
    F.col("price_change_24h"),
    F.col("price_change_percentage_24h"),
    F.col("circulating_supply"),
    F.col("total_supply")
).filter(F.col("coin_id").isNotNull())

fact_path = f"{GOLD_PATH}fact_crypto_daily/"

try:
    df_existing_fact = spark.read.parquet(fact_path)
    print(f"Existing fact table has {df_existing_fact.count()} records")
    
    df_existing_fact_filtered = df_existing_fact.filter(F.col("date") != today)
    
    fact_final = df_existing_fact_filtered.union(fact_new)
    
    print(f"After merge: {fact_final.count()} total records")
    
except:
    print("First run - creating new fact table")
    fact_final = fact_new

(
    fact_final.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("date")
        .save(fact_path)
)

dim_coins_path = f"{GOLD_PATH}dim_coins/"

dim_coins_new = df_new.select(
    F.col("coin_id"),
    F.col("symbol"),
    F.col("name")
).distinct()

try:
    dim_coins_existing = spark.read.parquet(dim_coins_path)
    
    dim_coins_final = dim_coins_existing.union(dim_coins_new).distinct()
    
    print(f"Dimension coins updated: {dim_coins_final.count()} total coins")
    
except:
    dim_coins_final = dim_coins_new

(
    dim_coins_final.write
        .mode("overwrite")
        .format("parquet")
        .save(dim_coins_path)
)

dim_date_path = f"{GOLD_PATH}dim_date/"

dim_date_new = df_new.select(
    F.col("update_date").alias("date")
).distinct().withColumn(
    "year", F.year(F.col("date"))
).withColumn(
    "month", F.month(F.col("date"))
).withColumn(
    "day", F.dayofmonth(F.col("date"))
).withColumn(
    "quarter", F.quarter(F.col("date"))
).withColumn(
    "day_of_week", F.dayofweek(F.col("date"))
).withColumn(
    "week_of_year", F.weekofyear(F.col("date"))
).withColumn(
    "month_name", F.date_format(F.col("date"), "MMMM")
).withColumn(
    "day_name", F.date_format(F.col("date"), "EEEE")
).withColumn(
    "is_weekend", F.when(F.dayofweek(F.col("date")).isin([1, 7]), True).otherwise(False)
)

try:
    dim_date_existing = spark.read.parquet(dim_date_path)
    
    dim_date_final = dim_date_existing.union(dim_date_new).distinct()
    
    print(f"Dimension date updated: {dim_date_final.count()} total dates")
    
except:
    dim_date_final = dim_date_new

(
    dim_date_final.write
        .mode("overwrite")
        .format("parquet")
        .save(dim_date_path)
)

print("Gold layer incremental update complete!")

job.commit()