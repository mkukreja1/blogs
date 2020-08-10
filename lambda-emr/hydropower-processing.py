from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row, Column
from pyspark.sql import functions as f
from dateutil import relativedelta
from datetime import timedelta
import argparse
import boto3

parser = argparse.ArgumentParser()
parser.add_argument('--JOB_DATE', dest='JOB_DATE')
parser.add_argument('--S3_BUCKET', dest='S3_BUCKET')
parser.add_argument('--REGION', dest='REGION')
args = parser.parse_args()
print(args)
JOB_DATE=args.JOB_DATE
S3_BUCKET=args.S3_BUCKET
REGION=args.REGION

READ_PATH='data/'+JOB_DATE
S3_READ_PATH='s3://'+S3_BUCKET+'/'+READ_PATH
WRITE_PATH='curated/'+JOB_DATE
S3_WRITE_PATH='s3://'+S3_BUCKET+'/'+WRITE_PATH

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def does_s3key_exist(bucket, key, ext):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    objects = bucket.objects.all()
    FOUND=0
    for object in objects:
        if object.key.startswith(key) and object.key.endswith(ext):
            FOUND=1
    return FOUND

if does_s3key_exist(S3_BUCKET, READ_PATH, '.csv') == 1:
    hydropower_consumption_df=spark.read.csv(S3_READ_PATH, header=True)
    hydropower_consumption_df=hydropower_consumption_df.withColumnRenamed("Hydropower (Terawatt-hours)",'consumption') \
                                                   .withColumn('Year', f.col('Year').cast(IntegerType())) \
                                                   .withColumn('consumption', f.col('consumption').cast(FloatType()))
    hydropower_consumption_df.show(5)
    hydropower_consumption_df.write.parquet(S3_WRITE_PATH)


