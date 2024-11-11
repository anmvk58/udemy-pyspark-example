import os

from pyspark.sql import SparkSession

from utils.utils import get_spark_app_config
from utils.logger import Log4j

# Path for spark source folder (add because env in os do not set)
os.environ["JAVA_HOME"] = "D:\\anmv2\\Environment\\Jdk1.8"
# os.environ["HADOOP_HOME"] = "D:\\anmv2\\Environment\\hadoop-3.3.6"
# os.environ["SPARK_HOME"] = "D:\\anmv2\\Environment\\Spark\\spark-3.5.3-bin-hadoop3"

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # config for logging
    logger = Log4j(spark)

    logger.info("Starting Read Dataframe via file types")

    flight_time_df = (spark.read
                      .format("parquet")
                      .load("data/"))

    # Create Spark Database
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # Create spark SQL Table into Database
    flight_time_df.write.mode("overwrite").saveAsTable("flight_data_tbl")

    # flight_time_df.printSchema()
    logger.info(spark.catalog.listTables("AIRLINE_DB"))