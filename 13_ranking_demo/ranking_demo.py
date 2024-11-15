import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from utils.utils import get_spark_app_config
from utils.logger import Log4j

# Path for spark source folder (add because env in os do not set)
# os.environ["JAVA_HOME"] = "D:\\anmv2\\Environment\\Jdk1.8"
# os.environ["HADOOP_HOME"] = "D:\\anmv2\\Environment\\hadoop-3.3.6"
# os.environ["SPARK_HOME"] = "D:\\anmv2\\Environment\\Spark\\spark-3.5.3-bin-hadoop3"


if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # config for logging
    logger = Log4j(spark)

    logger.info("Example for ranking your data")

    summary_df = spark.read \
        .format("parquet") \
        .option("path", "data/summary.parquet") \
        .load()

    summary_df.sort("Country", "WeekNumber").show()

    rank_window = Window.partitionBy("Country") \
        .orderBy(f.col("InvoiceValue").desc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df = summary_df.withColumn("Rank", f.dense_rank().over(rank_window)).sort("Country", f.col("InvoiceValue").desc())
    df.show()
