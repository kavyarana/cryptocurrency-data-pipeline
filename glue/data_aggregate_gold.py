import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

SILVER_PATH = "s3://kavyabd/silverstaging/"
GOLD_PATH = "s3://kavyabd/goldlayer/"

print("GOLD AGGREGATION JOB STARTED")

# Read all data from Silver
print("Reading from Silver layer...")
df_silver = spark.read.parquet(SILVER_PATH)

total_records = df_silver.count()
print(f"Total records in Silver: {total_records}")

if total_records == 0:
    print("No data in Silver layer - skipping Gold aggregation")
    job.commit()
    sys.exit(0)

# Process last 7 days of data (or all if less than 7 days)
cutoff_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
print(f"Processing data from {cutoff_date} onwards")

df_recent = df_silver.filter(F.col("update_date") >= cutoff_date)
recent_count = df_recent.count()

if recent_count == 0:
    print(f"No data found after {cutoff_date}, processing all available data")
    df_recent = df_silver

print(f"Processing {df_recent.count()} records")

# Show date distribution
print("Data distribution by date:")
df_recent.groupBy("update_date").count().orderBy("update_date").show()

# CREATE FACT TABLE
print("Creating fact_crypto_daily...")
fact_new = df_recent.select(
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
    existing_count = df_existing_fact.count()
    print(f"Existing fact table: {existing_count} records")
    
    # Get distinct dates from new data
    new_dates = [row.date for row in fact_new.select("date").distinct().collect()]
    print(f"Updating data for dates: {new_dates}")
    
    # Remove old data for these dates, then add new data
    df_existing_fact_filtered = df_existing_fact.filter(~F.col("date").isin(new_dates))
    fact_final = df_existing_fact_filtered.union(fact_new)
    
    print(f"After merge: {fact_final.count()} total records")
    
except Exception as e:
    print(f"No existing fact table or read error: {str(e)}")
    print("Creating new fact table from scratch")
    fact_final = fact_new

# Write fact table
print(f"Writing fact table with {fact_final.count()} records...")
fact_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .partitionBy("date") \
    .save(fact_path)

print(f"Fact table written successfully")

# CREATE DIM_COINS
print("Creating dim_coins...")
dim_coins_path = f"{GOLD_PATH}dim_coins/"

dim_coins_new = df_recent.select(
    F.col("coin_id"),
    F.col("symbol"),
    F.col("name")
).distinct()

new_coins_count = dim_coins_new.count()
print(f"New coins to add: {new_coins_count}")

try:
    dim_coins_existing = spark.read.parquet(dim_coins_path)
    existing_coins = dim_coins_existing.count()
    print(f"Existing coins: {existing_coins}")
    
    dim_coins_final = dim_coins_existing.union(dim_coins_new).distinct()
    final_coins = dim_coins_final.count()
    print(f"After merge: {final_coins} unique coins")
    
except Exception as e:
    print(f"No existing dim_coins or read error: {str(e)}")
    print("Creating new dim_coins from scratch")
    dim_coins_final = dim_coins_new

print(f"Writing dim_coins with {dim_coins_final.count()} records...")
dim_coins_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(dim_coins_path)

print(f"Dim_coins written successfully")

# CREATE DIM_DATE
print("Creating dim_date...")
dim_date_path = f"{GOLD_PATH}dim_date/"

dim_date_new = df_recent.select(
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

new_dates_count = dim_date_new.count()
print(f"New dates to add: {new_dates_count}")

try:
    dim_date_existing = spark.read.parquet(dim_date_path)
    existing_dates = dim_date_existing.count()
    print(f"Existing dates: {existing_dates}")
    
    dim_date_final = dim_date_existing.union(dim_date_new).distinct()
    final_dates = dim_date_final.count()
    print(f"After merge: {final_dates} unique dates")
    
except Exception as e:
    print(f"No existing dim_date or read error: {str(e)}")
    print("Creating new dim_date from scratch")
    dim_date_final = dim_date_new

print(f"Writing dim_date with {dim_date_final.count()} records...")
dim_date_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(dim_date_path)

print(f" Dim_date written successfully")

print("=" * 60)
print("GOLD LAYER SUMMARY")
print("=" * 60)
fact_count = spark.read.parquet(fact_path).count()
coins_count = spark.read.parquet(dim_coins_path).count()
dates_count = spark.read.parquet(dim_date_path).count()
print(f"fact_crypto_daily: {fact_count} records")
print(f"dim_coins: {coins_count} coins")
print(f"dim_date: {dates_count} dates")
print("=" * 60)

print("Gold layer aggregation completed successfully!")

job.commit()
