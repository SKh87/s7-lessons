import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local") \
    .appName("Learning DataFrames") \
    .getOrCreate()

events = spark.read.json("/user/master/data/events/date=2022-05-01")

window = Window().partitionBy('event.message_from').orderBy('event.datetime')

dfWithLag = events.withColumn("lag_7", F.lag("event.message_to", 7).over(window))

dfWithLag.select('event.message_from', "lag_7") \
    .filter(dfWithLag.lag_7.isNotNull()) \
    .orderBy(F.desc('event.message_to')) \
    .show(10, False)