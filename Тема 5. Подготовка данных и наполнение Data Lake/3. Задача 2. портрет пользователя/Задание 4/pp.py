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

tmp = spark.read.json(path="/user/master/data/events/date=2022-05-31")

tmp.filter(F.col("event_type")=='subscription').show(truncate=False)