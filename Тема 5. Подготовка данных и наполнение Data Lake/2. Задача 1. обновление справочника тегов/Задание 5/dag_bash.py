from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

# dag_bahs_spark_submit = DAG(
#     dag_id="sparkoperator",
#     default_args=default_args,
#     schedule_interval=None,
# )

with DAG(dag_id="sparkoperator",
         default_args=default_args,
         schedule_interval=None,
         ) as dag:

    spark_submit_local = BashOperator(
        task_id='events',
        bash_command='spark-submit --master yarn --conf spark.driver.maxResultSize=2g --executor-cores 2 --executor-memory 2g --name arrow-spark /lessons/partition.py 2020-05-01 /user/master/data/events /user/sergeykhar/data/events',
        retries=1
    )

    tags_d84 = BashOperator(
        task_id='tags_d84',
        bash_command='spark-submit --master yarn --deploy-mode cluster /lessons/verified_tags_candidates.py 2022-05-31 84 1000 /user/sergeykhar/data/events /user/master/data/snapshots/tags_verified/actual /user/sergeykhar/5.2.4/analytics/verified_tags_candidates_d84',
        retries=1
    )

    tags_d7 = BashOperator(
        task_id='tags_d7',
        bash_command='spark-submit --master yarn --deploy-mode cluster /lessons/verified_tags_candidates.py 2022-05-31 7 100 /user/sergeykhar/data/events /user/master/data/snapshots/tags_verified/actual /user/sergeykhar/5.2.4/analytics/verified_tags_candidates_d7',
        retries=1
    )

    spark_submit_localX = SparkSubmitOperator(
        task_id='spark_submit_task',
        application='/lessons/partition.py',
        conn_id='yarn_spark',
        application_args=["2020-05-01", "/user/master/data/events", "/user/sergeykhar/data/events"],
        conf={
            "spark.driver.maxResultSize": "2g"
        },
        executor_cores=2,
        executor_memory='2g'
    )

    spark_submit_local >> tags_d7
    spark_submit_local >> tags_d84

spark_submit_local
