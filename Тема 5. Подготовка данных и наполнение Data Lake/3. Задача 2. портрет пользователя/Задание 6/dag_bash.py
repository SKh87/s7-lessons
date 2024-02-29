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


    user_interests_d2022_05_25_7 = BashOperator(
        task_id='user_interests_d2022_05_25_7',
        bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/user_interests.py 2022-05-04 5 /user/sergeykhar/data/events /user/sergeykhar/analytics/user_interests_d2022-05-25_7',
        retries=1
    )

    user_interests_d2022_05_25_28 = BashOperator(
        task_id='user_interests_d2022_05_25_28',
        bash_command='/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/user_interests.py 2022-05-04 5 /user/sergeykhar/data/events /user/sergeykhar/analytics/user_interests_d2022-05-25_28',
        retries=1
    )

    user_interests_d2022_05_25_7
    user_interests_d2022_05_25_28


