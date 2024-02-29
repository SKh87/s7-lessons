from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
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

dag_bahs_spark_submit = DAG(
    dag_id="sparkoperator",
    default_args=default_args,
    schedule_interval=None,
)

spark_submit_local = BashOperator(
    task_id='print_date',
    bash_command='spark-submit --master yarn --conf spark.driver.maxResultSize=2g --executor-cores 2 --executor-memory 2g --name arrow-spark /lessons/partition.py 2020-05-01 /user/master/data/events /user/sergeykhar/data/events',
    retries=1,
    dag=dag_bahs_spark_submit
)

spark_submit_local
