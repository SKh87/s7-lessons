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
events = spark.read.json("/user/master/data/events/date=2022-05-31")
events_curr_day = events \
    .withColumn('hour', F.hour(F.col("event.datetime"))) \
    .withColumn('minute', F.minute(F.col("event.datetime"))) \
    .withColumn('second', F.second(F.col("event.datetime"))) \
    .orderBy(F.col("event.datetime").desc())
    # .orderBy(events.event.datetime.desc())
events_curr_day.show(10, True)
