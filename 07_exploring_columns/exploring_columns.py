import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import column, col, to_date, concat

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
                      .load("data/*.parquet"))

    # select by 2 ways:
    # flight_time_df.select("Origin", "Dest", "Distance").show(10)

    # flight_time_df.select(column("Origin"), col("Dest"), "Distance").show(10)

    # flight_time_df.select("Origin", "Dest", "Distance", "Year", "Month", "DayofMonth").show(10)

    # flight_time_df.selectExpr("Origin", "Dest", "Distance", "to_date(concat(Year, Month, DayofMonth),'yyyyMMdd') as FlightDate").show(10)

    # flight_time_df.select("Origin", "Dest", "Distance", to_date(concat("Year", "Month", "DayofMonth"), "yyyyMMdd").alias("FlightDate")).show(10)

