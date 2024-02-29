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


# reaction_type
def reaction_tag_tops(date: str, depth: int, spark: SparkSession) -> DataFrame:
    df = spark.read.option("basePath", f"/user/{STUDENT_USER_NAME}/data/events") \
        .parquet(*input_paths(date, depth))

    window = Window.partitionBy("user_id", "reaction_type").orderBy(F.desc("cnt"), F.desc("tag"))

    df = df. \
        where(f"event_type='reaction'"). \
        selectExpr(
        ["event.reaction_from as user_id", "explode_outer(event.tags) as tag", "event.reaction_type as reaction_type"]). \
        groupBy("user_id", "tag", "reaction_type"). \
        count().withColumnRenamed("count", "cnt"). \
        withColumn("rn", F.row_number().over(window)). \
        where("rn<=3"). \
        withColumn("x", F.concat(F.col("reaction_type"), F.lit("_top_"), F.col("rn"))). \
        drop("rn", "cnt"). \
        groupBy("user_id"). \
        pivot("x", ["like_tag_top_1", "like_tag_top_2", "like_tag_top_3", "dislike_tag_top_1", "dislike_tag_top_2",
                    "dislike_tag_top_3"]). \
        agg(F.first("tag"))

    df.printSchema()

    return df


def reaction_tag_tops(date, depth, spark):
    reaction_paths = input_paths(date, depth)
    reactions = spark.read \
        .option("basePath", f"/user/{STUDENT_USER_NAME}/data/events") \
        .parquet(*reaction_paths) \
        .where("event_type='reaction'")

    all_message_tags = spark.read.parquet(f"/user/{STUDENT_USER_NAME}/data/events") \
        .where("event_type='message' and event.message_channel_to is not null") \
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag")
                )

    reaction_tags = reactions \
        .select(F.col("event.reaction_from").alias("user_id"),
                F.col("event.message_id").alias("message_id"),
                F.col("event.reaction_type").alias("reaction_type")
                ).join(all_message_tags.select("message_id", "tag"), "message_id")

    reaction_tops = reaction_tags \
        .groupBy("user_id", "tag", "reaction_type") \
        .agg(F.count("*").alias("tag_count")) \
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type") \
                                                .orderBy(F.desc("tag_count"), F.desc("tag")))) \
        .where("rank <= 3") \
        .groupBy("user_id", "reaction_type") \
        .pivot("rank", [1, 2, 3]) \
        .agg(F.first("tag")) \
        .cache()

    like_tops = reaction_tops \
        .where("reaction_type = 'like'") \
        .drop("reaction_type") \
        .withColumnRenamed("1", "like_tag_top_1") \
        .withColumnRenamed("2", "like_tag_top_2") \
        .withColumnRenamed("3", "like_tag_top_3")

    dislike_tops = reaction_tops \
        .where("reaction_type = 'dislike'") \
        .drop("reaction_type") \
        .withColumnRenamed("1", "dislike_tag_top_1") \
        .withColumnRenamed("2", "dislike_tag_top_2") \
        .withColumnRenamed("3", "dislike_tag_top_3")

    result = like_tops \
        .join(dislike_tops, "user_id", "full_outer")

    return result


def calculate_user_interests(date, depth, spark):
    tt = tag_tops(date, depth, spark).repartition(1)
    rtt = reaction_tag_tops(date, depth, spark)

    result = tt.join(rtt, "user_id", "full_outer")

    return result


spark = SparkSession.builder \
    .master("yarn") \
    .appName("Learning DataFrames") \
    .getOrCreate()

calculate_user_interests('2022-05-31', 7, spark).write.mode("overwrite").parquet(
    f'/user/{STUDENT_USER_NAME}/data/analytics/user_interests_05_31_7')
calculate_user_interests('2022-05-31', 28, spark).write.mode("overwrite").parquet(
    f'/user/{STUDENT_USER_NAME}/data/analytics/user_interests_05_31_28')

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


# reaction_type
def reaction_tag_tops(date: str, depth: int, spark: SparkSession) -> DataFrame:
    df = spark.read.option("basePath", f"/user/{STUDENT_USER_NAME}/data/events") \
        .parquet(*input_paths(date, depth))

    window = Window.partitionBy("user_id", "reaction_type").orderBy(F.desc("cnt"), F.desc("tag"))

    df = df. \
        where(f"event_type='reaction'"). \
        selectExpr(
        ["event.reaction_from as user_id", "explode_outer(event.tags) as tag", "event.reaction_type as reaction_type"]). \
        groupBy("user_id", "tag", "reaction_type"). \
        count().withColumnRenamed("count", "cnt"). \
        withColumn("rn", F.row_number().over(window)). \
        where("rn<=3"). \
        withColumn("x", F.concat(F.col("reaction_type"), F.lit("_top_"), F.col("rn"))). \
        drop("rn", "cnt"). \
        groupBy("user_id"). \
        pivot("x", ["like_tag_top_1", "like_tag_top_2", "like_tag_top_3", "dislike_tag_top_1", "dislike_tag_top_2",
                    "dislike_tag_top_3"]). \
        agg(F.first("tag"))

    df.printSchema()

    return df


def reaction_tag_tops(date, depth, spark):
    reaction_paths = input_paths(date, depth)
    reactions = spark.read \
        .option("basePath", f"/user/{STUDENT_USER_NAME}/data/events") \
        .parquet(*reaction_paths) \
        .where("event_type='reaction'")

    all_message_tags = spark.read.parquet(f"/user/{STUDENT_USER_NAME}/data/events") \
        .where("event_type='message' and event.message_channel_to is not null") \
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag")
                )

    reaction_tags = reactions \
        .select(F.col("event.reaction_from").alias("user_id"),
                F.col("event.message_id").alias("message_id"),
                F.col("event.reaction_type").alias("reaction_type")
                ).join(all_message_tags.select("message_id", "tag"), "message_id")

    reaction_tops = reaction_tags \
        .groupBy("user_id", "tag", "reaction_type") \
        .agg(F.count("*").alias("tag_count")) \
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type") \
                                                .orderBy(F.desc("tag_count"), F.desc("tag")))) \
        .where("rank <= 3") \
        .groupBy("user_id", "reaction_type") \
        .pivot("rank", [1, 2, 3]) \
        .agg(F.first("tag")) \
        .cache()

    like_tops = reaction_tops \
        .where("reaction_type = 'like'") \
        .drop("reaction_type") \
        .withColumnRenamed("1", "like_tag_top_1") \
        .withColumnRenamed("2", "like_tag_top_2") \
        .withColumnRenamed("3", "like_tag_top_3")

    dislike_tops = reaction_tops \
        .where("reaction_type = 'dislike'") \
        .drop("reaction_type") \
        .withColumnRenamed("1", "dislike_tag_top_1") \
        .withColumnRenamed("2", "dislike_tag_top_2") \
        .withColumnRenamed("3", "dislike_tag_top_3")

    result = like_tops \
        .join(dislike_tops, "user_id", "full_outer")

    return result


def calculate_user_interests(date, depth, spark):
    tt = tag_tops(date, depth, spark).repartition(1)
    rtt = reaction_tag_tops(date, depth, spark)

    result = tt.join(rtt, "user_id", "full_outer")

    return result


spark = SparkSession.builder \
    .master("yarn") \
    .appName("Learning DataFrames") \
    .getOrCreate()

def compare_df(df1, df2: DataFrame):
    diff=df1.unionAll(df2).exceptAll(df1.intersect(df2))
    return diff.count() == 0

df1 = spark.read.parquet(f'/user/{STUDENT_USER_NAME}/data/analytics/user_interests_05_31_7')
df2 = spark.read.parquet(f'/user/examples/data/interests_d7')
print(compare_df(df1,df2))

df1 = spark.read.parquet(f'/user/{STUDENT_USER_NAME}/data/analytics/user_interests_05_31_28')
df2 = spark.read.parquet(f'/user/examples/data/interests_d28/user_interests_d28')
print(compare_df(df1,df2))
