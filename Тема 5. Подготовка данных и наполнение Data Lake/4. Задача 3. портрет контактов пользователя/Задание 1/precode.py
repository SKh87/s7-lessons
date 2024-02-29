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
        s = f"/user/{STUDENT_USER_NAME}/data/events/date={dstr}"
        res.append(s)
        d = d - timedelta(days=1)
    return res


def tag_tops(date: str, depth: int, spark: SparkSession) -> DataFrame:
    df = spark.read.option("basePath", f"/user/{STUDENT_USER_NAME}/data/events") \
        .parquet(*input_paths(date, depth))

    window = Window.partitionBy("user_id").orderBy(F.desc("cnt"), F.desc("tag"))

    df = df. \
        where(f"event_type='message'"). \
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

df = spark.read.option("basePath", f"/user/{STUDENT_USER_NAME}/data/events") \
    .parquet(*input_paths("2022-05-25", 7))

df.show(truncate=False)
df.select("event_type").distinct().show(truncate=False)
df.filter(F.col("event_type") == 'message').show(truncate=False)

direct_messages = df. \
    filter(F.col("event_type") == 'message'). \
    filter(F.col("event.message_to").isNotNull()). \
    select(
    F.col("event.message_to").alias("message_to"),
    F.col("event.message_from").alias("message_from")). \
    distinct(). \
    cache()

# direct_messages.show(truncate=False)
# df.printSchema()

# direct_messages.count()

direct_messages = direct_messages.union(direct_messages.select("message_from", "message_to")).distinct()

# direct_messages.count()
# direct_messages.distinct().count()

direct_messages.write.option("overwrite", True).parquet("/user/sergeykhar/tmp/direct_messages_d2022-05-25")

interests = spark.read.parquet("/user/sergeykhar/analytics/user_interests_d2022-05-25")

direct_messages.join(interests, F.col("message_from") == F.col("user_id")). \
    drop("message_from", "user_id"). \
    withColumnRenamed("message_to", "user_id"). \
    withColumn("like_tag_top_1_count", F.count("like_tag_top_1").over(Window.partitionBy("user_id", "like_tag_top_1"))).\
    show(truncate=False)
