import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("yarn") \
    .appName("Learning DataFrames") \
    .getOrCreate()

channelsDF = spark.read.load(path="/user/master/data/snapshots/channels/actual")
channelsDF.show(10)

channelsDF.write.option("header", True).partitionBy("channel_type").mode("append").parquet(
    "/user/sergeykhar/analytics/test")


myChannelsDF = spark.read.load("/user/sergeykhar/analytics/test")
myChannelsDF.select("channel_type").orderBy("channel_type").distinct().show()