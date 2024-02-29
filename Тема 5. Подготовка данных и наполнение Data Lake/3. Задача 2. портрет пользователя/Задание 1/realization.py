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

def input_paths(date: str, depth: int):
    res = list()
    d = datetime.strptime(date, "%Y-%m-%d").date()
    for i in range(depth):
        dstr = d.strftime("%Y-%m-%d")
        s = f"/user/{STUDENT_USER_NAME}/data/events/date={dstr}/event_type=message"
        res.append(s)
        d = d - timedelta(days=1)
    return res

def tag_tops(date: str, depth: int, spark: SparkSession)-> DataFrame:
    df = spark.read.parquet(*input_paths(date, depth))

    window = Window.partitionBy("user_id").orderBy(F.desc("cnt"), F.desc("tag"))

    df = df. \
        selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"]). \
        groupBy("user_id", "tag"). \
        count().withColumnRenamed("count", "cnt"). \
        withColumn("rn", F.row_number().over(window)). \
        where("rn<=3"). \
        withColumn("x", F.concat(F.lit("tag_top_"), F.col("rn"))). \
        drop("rn", "cnt"). \
        groupBy("user_id"). \
        pivot("x", ["tag_top_1", "tag_top_2", "tag_top_3"]). \
        agg(F.first("tag"))

    return df


spark = SparkSession.builder \
    .master("yarn") \
    .appName("Learning DataFrames") \
    .getOrCreate()

tag_tops('2022-06-04', 5, spark).repartition(1).write.parquet(f'/user/{STUDENT_USER_NAME}/data/tmp/tag_tops_06_04_5')
tag_tops('2022-05-04', 5, spark).repartition(1).write.parquet(f'/user/{STUDENT_USER_NAME}/data/tmp/tag_tops_05_04_5')
tag_tops('2022-05-04', 1, spark).repartition(1).write.parquet(f'/user/{STUDENT_USER_NAME}/data/tmp/tag_tops_05_04_1')

# # данные  датафрейма
# json_string = """
# [{"event":{"message_from":"1", "tags":['tag1','tag2']}},
# {"event":{"message_from":"2", "tags":['tag3','tag4']}}]
# """
#
# # Convert the JSON string into an RDD
# rdd = spark.sparkContext.parallelize([json_string])
#
# # Read the RDD as a JSON into a DataFrame
# df = spark.read.json(rdd)
#
#
# df = spark.read.parquet(*input_paths("2022-06-04",5))
#
# window = Window.partitionBy("user_id").orderBy(F.desc("cnt"), F.desc("tag"))
#
# df = df. \
#     selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"]). \
#     groupBy("user_id", "tag"). \
#     count().withColumnRenamed("count", "cnt"). \
#     withColumn("rn", F.row_number().over(window)). \
#     where("rn<=3"). \
#     withColumn("x", F.concat(F.lit("tag_top_"), F.col("rn"))). \
#     drop("rn", "cnt"). \
#     groupBy("user_id"). \
#     pivot("x", ["tag_top_1","tag_top_2","tag_top_3"]). \
#     agg(F.first("tag"))
#
# df.show()