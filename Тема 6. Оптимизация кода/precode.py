import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from datetime import datetime, timedelta

STUDENT_USER_NAME = "sergeykhar"


spark = SparkSession.builder \
    .master("yarn") \
    .appName("broadcast join") \
    .getOrCreate()

users = spark.read.load("/user/examples/users_small")
events = spark.read.load("/user/sergeykhar/data/events")

users.printSchema()
events.printSchema()

df= events.join(users, F.col("event.message_from")==F.col("id"))



# Измените этот код:
res = user_bd.select("id").groupBy("id") \
.join(events.select("event.*"), F.col("id") == F.col("message_from"), "left_outer")\
.agg(F.count(F.when(F.expr("message like '%birthday%'"), F.lit(1))).alias("count"))