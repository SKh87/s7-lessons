import os
import sys
from datetime import datetime, timedelta

from pyspark import SparkConf, SparkContext, SQLContext

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

import pyspark.sql.functions as F


def input_paths(src_event_path: str, date: str, depth: int):
    res = list()
    d = datetime.strptime(date, "%Y-%m-%d").date()
    for i in range(depth):
        dstr = d.strftime("%Y-%m-%d")
        s = f"{src_event_path}/date={dstr}/event_type=message"
        res.append(s)
        d = d - timedelta(days=1)
    return res


def main():
    #Определяем параметры запуска
    date = sys.argv[1]
    depth = int(sys.argv[2])
    suggested_count = int(sys.argv[3])
    src_event_path = sys.argv[4]
    snapshots_tags_verified_path = sys.argv[5]
    dst_tags_path = sys.argv[6]

    # Список партиций за нужный период
    lists = input_paths(src_event_path, date, depth)

    # Определяем spark контекст
    conf = SparkConf().setAppName(f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{suggested_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # Датаа фрейм из всех партиций
    df = sql.read.parquet(*lists)
    # Дата фрейм стандартныйх тэгов
    tags_verified = sql.read.parquet(snapshots_tags_verified_path)

    # Считаем количество уникальных пользователей в разрезе тегов
    df = df.withColumn("tag", F.explode("event.tags")). \
        select("event.message_from", "tag"). \
        distinct(). \
        groupBy("tag"). \
        count().withColumnRenamed("count", "suggested_count")

    # Применяем огранияения и записываем в целевой файл
    df.where(f"suggested_count >= {suggested_count}"). \
        join(tags_verified, on=["tag"], how='anti'). \
        write.mode("overwrite").parquet(dst_tags_path)


if __name__ == "__main__":
    main()
