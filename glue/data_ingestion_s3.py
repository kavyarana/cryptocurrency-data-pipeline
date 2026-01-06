import sys
import json
import urllib.request
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET_NAME = 'kavyabd'
OUTPUT_PREFIX = 'rawbronze/rawtoprocess'

url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1'

with urllib.request.urlopen(url) as response:
    data = json.loads(response.read().decode('utf-8'))

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_path = f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/crypto_data_{timestamp}.json"

rdd = sc.parallelize(data)
df = spark.read.json(rdd.map(lambda x: json.dumps(x)))

df.coalesce(1).write.mode('overwrite').json(output_path)

print(f"Data ingested successfully to {output_path}")

job.commit()