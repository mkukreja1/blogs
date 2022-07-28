import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, array, ArrayType, DateType
from pyspark.sql import Row, Column
import datetime
import json
import boto3
import logging
import calendar
import uuid
import time
from dateutil import relativedelta
from datetime import timedelta
import argparse

def compare_dataframe(df1, df2):
    '''
    function: compare_dataframe
    description: Compares two dataframes and show their differences.

    Args:
        df1: spark.sql.dataframe - First dataframe to be compared.
        df2: spark.sql.dataframe - Second dataframe to be compared.

    returns:
        df1subdf2: spark.sql.dataframe - Different rows of df1.
        df2subdf1: spark.sql.dataframe - Different rows of df2.
    '''
    df1subdf2 = df1.subtract(df2)
    df2subdf1 = df2.subtract(df1)

    print('Different rows in first dataframe.')
    df1subdf2.show()

    print('Different rows in second dataframe.')
    df2subdf1.show()

    return df1subdf2, df2subdf1

parser = argparse.ArgumentParser()
parser.add_argument('--batch')
parser.add_argument('--bucket')
parser.add_argument('--lower_bound')
parser.add_argument('--upper_bound')
parser.add_argument('--table_name')
parser.add_argument('--oracle_data_path')
parser.add_argument('--postgres_data_path')
args = parser.parse_args()

BATCH = args.batch
BUCKET = args.bucket
LOWER_BOUND = args.lower_bound
UPPER_BOUND = args.upper_bound
TABLE_NAME = args.table_name
ORACLE_DATA_PATH = 's3://'+BUCKET+'/'+args.oracle_data_path
POSTGRES_DATA_PATH = 's3://'+BUCKET+'/'+args.postgres_data_path

print(ORACLE_DATA_PATH)
print(POSTGRES_DATA_PATH)

UPDATED=datetime.datetime.today().replace(second=0, microsecond=0)
SUMMARY_RECORDS = 's3://'+BUCKET+'/summary_records/'+TABLE_NAME+'_'+str(LOWER_BOUND)+'_'+str(UPPER_BOUND)
INVALID_RECORDS = 's3://'+BUCKET+'/invalid_records/'+TABLE_NAME+'_'+str(LOWER_BOUND)+'_'+str(UPPER_BOUND)
SUMMARY_RECORDS_COLUMNS = ['table_name', 'lower_bound', 'upper_bound', 'status', 'timestamp', 'total_count', 'failed_count']

sc = SparkContext.getOrCreate()

if BATCH:
    spark = SparkSession(sc)

print(ORACLE_DATA_PATH)
print(POSTGRES_DATA_PATH)

df_oracle=spark.read.parquet(ORACLE_DATA_PATH)
df_postgres=spark.read.parquet(POSTGRES_DATA_PATH)

#df_oracle.show(5)
#df_postgres.show(5)

df_compare_oracle, df_compare_postgres = compare_dataframe(df_oracle,df_postgres)

ORACLE_COMPARE_COUNT=df_compare_oracle.count()
POSTGRES_COMPARE_COUNT=df_compare_postgres.count()
print(ORACLE_COMPARE_COUNT)
print(POSTGRES_COMPARE_COUNT)

ORACLE_COUNT=df_oracle.count()
print(ORACLE_COUNT)

if (ORACLE_COMPARE_COUNT == 0 & POSTGRES_COMPARE_COUNT == 0 ):
    status="Success"
    SUMMARY_RECORDS_VALS = [ ("dnatag.ligands_stats", LOWER_BOUND, UPPER_BOUND, status,UPDATED,ORACLE_COUNT, 0)]
else:
    status="Failure"
    count=max(ORACLE_COMPARE_COUNT, POSTGRES_COMPARE_COUNT)
    SUMMARY_RECORDS_VALS = [ ("dnatag.ligands_stats", LOWER_BOUND, UPPER_BOUND, status,UPDATED, ORACLE_COUNT, count)]
    df_compare_oracle.repartition(1).write.option("delimiter", ',').option("header", "false").option("quoteAll", "true").option("quote", "\"").csv(INVALID_RECORDS+'/oracle')
    df_compare_postgres.repartition(1).write.option("delimiter", ',').option("header", "false").option("quoteAll", "true").option("quote", "\"").csv(INVALID_RECORDS+'/postgres')

df_output = spark.createDataFrame(SUMMARY_RECORDS_VALS, SUMMARY_RECORDS_COLUMNS)
df_output.show()

df_output.repartition(1).write.option("delimiter", ',').option("header", "false").option("quoteAll", "true").option("quote", "\"").csv(SUMMARY_RECORDS)
