import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("yarn") \
    .appName("Learning DataFrames") \
    .getOrCreate()

tmp = spark.read.json(path="/user/master/data/events")

tmp.write.option("header", True). \
    partitionBy("event.message_ts"). \
    partitionBy("event_type"). \
    mode("append"). \
    parquet("/user/sergeykhar/data/events")


spark.read.parquet("/user/sergeykhar/data/events").orderBy(F.desc("event.datetime")).show(10)
