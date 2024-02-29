import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from datetime import datetime, timedelta


def input_paths(date: str, depth: int):
    res = list()
    d = datetime.strptime(date, "%Y-%m-%d").date()
    for i in range(depth):
        dstr = d.strftime("%Y-%m-%d")
        s = f"/user/sergeykhar/data/events/date={dstr}/event_type=message"
        res.append(s)
        d = d - timedelta(days=1)
    return res


spark = SparkSession.builder \
    .master("yarn") \
    .appName("Learning DataFrames") \
    .getOrCreate()

df: DataFrame
for i, path in enumerate(input_paths("2022-05-31", 7)):
    if i == 0:
        df = spark.read.parquet(path)
    else:
        df = df.union(spark.read.parquet(path))

tags_verified = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
df=df.withColumn("tag", F.explode("event.tags")). \
    select("event.message_from", "tag"). \
    distinct(). \
    groupBy("tag"). \
    count().withColumnRenamed("count", "suggested_count")

df.where("suggested_count >= 100").\
    join(tags_verified, on=["tag"], how='anti').drop("tag"). \
    write.mode("overwrite").parquet("/user/sergeykhar/data/analytics/candidates_d7_pyspark")


