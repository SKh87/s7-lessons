from pyspark.sql import SparkSession

spark = (
    SparkSession.builder  # создаём объект Spark-сессии, обращаясь к объекту builder, который создаёт сессию, учитывая параметры конфигурации
    .master("yarn")  # явно указываем, что хотим запустить Spark в локальном режиме
    .appName("My second session")  # задаём название нашего Spark-приложения
    .getOrCreate()  # функция инициализации объекта сессии
    )
