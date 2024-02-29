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
events = spark.read.json("/user/master/data/events/date=2022-05-25")
event_reaction_from = events.filter(F.col('event_type') == 'reaction').groupBy(F.col('event.reaction_from')).count()
event_reaction_from.select(F.max('count')).show()
