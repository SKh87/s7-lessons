import os
import sys

from pyspark import SparkConf, SparkContext, SQLContext
#
# os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
#
# import findspark
#
# findspark.init()
# findspark.find()

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from datetime import datetime, timedelta

STUDENT_USER_NAME = "sergeykhar"


def input_paths(base_path: str, date: str, depth: int):
    res = list()
    d = datetime.strptime(date, "%Y-%m-%d").date()
    for i in range(depth):
        dstr = d.strftime("%Y-%m-%d")
        # s = f"/user/{STUDENT_USER_NAME}/data/events/date={dstr}"
        s = f"{base_path}/date={dstr}"
        res.append(s)
        d = d - timedelta(days=1)
    return res


def tag_tops(base_path: str, date: str, depth: int, spark: SparkSession) -> DataFrame:
    df = spark.read.option("basePath", f"{base_path}") \
        .parquet(*input_paths(base_path, date, depth))

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
# def reaction_tag_tops(base_path: str, date: str, depth: int, spark: SparkSession) -> DataFrame:
#     df = spark.read.option("basePath", f"{base_path}") \
#         .parquet(*input_paths(base_path, date, depth))
#
#     window = Window.partitionBy("user_id", "reaction_type").orderBy(F.desc("cnt"), F.desc("tag"))
#
#     df = df. \
#         where(f"event_type='reaction'"). \
#         selectExpr(
#         ["event.reaction_from as user_id", "explode_outer(event.tags) as tag", "event.reaction_type as reaction_type"]). \
#         groupBy("user_id", "tag", "reaction_type"). \
#         count().withColumnRenamed("count", "cnt"). \
#         withColumn("rn", F.row_number().over(window)). \
#         where("rn<=3"). \
#         withColumn("x", F.concat(F.col("reaction_type"), F.lit("_top_"), F.col("rn"))). \
#         drop("rn", "cnt"). \
#         groupBy("user_id"). \
#         pivot("x", ["like_tag_top_1", "like_tag_top_2", "like_tag_top_3", "dislike_tag_top_1", "dislike_tag_top_2",
#                     "dislike_tag_top_3"]). \
#         agg(F.first("tag"))
#
#     df.printSchema()
#
#     return df


def reaction_tag_tops(base_path: str, date: str, depth: int, spark: SparkSession) -> DataFrame:
    reaction_paths = input_paths(date, depth)
    reactions = spark.read \
        .option("basePath", f"{base_path}") \
        .parquet(*reaction_paths) \
        .where("event_type='reaction'")

    all_message_tags = spark.read.parquet(f"{base_path}") \
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


def calculate_user_interests(base_path: str, date: str, depth: int, spark: SparkSession) -> DataFrame:
    tt = tag_tops(base_path, date, depth, spark).repartition(1)
    rtt = reaction_tag_tops(base_path, date, depth, spark)

    result = tt.join(rtt, "user_id", "full_outer")

    return result


def compare_df(df1, df2: DataFrame):
    diff1_2 = df1.exceptAll(df2)
    diff2_1 = df2.exceptAll(df1)
    return diff1_2.count() == 0 and diff2_1.count() == 0


def main():
    date = sys.argv[1]
    days_count = int(sys.argv[2])
    events_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    conf = SparkConf().setAppName(f"UserInterestsJob-{date}-{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    df=calculate_user_interests(base_path=events_base_path, date=date, depth=days_count, spark=sql)
    df.write.mode("overwrite").parquet(f"{output_base_path}")

if __name__ == "__main__":
    main()
