import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from utils.utils import get_spark_app_config
from utils.logger import Log4j

# Path for spark source folder (add because env in os do not set)
os.environ["JAVA_HOME"] = "D:\\anmv2\\Environment\\Jdk1.8"
os.environ["HADOOP_HOME"] = "D:\\anmv2\\Environment\\hadoop-3.3.6"
os.environ["SPARK_HOME"] = "D:\\anmv2\\Environment\\Spark\\spark-3.5.3-bin-hadoop3"

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # config for logging
    logger = Log4j(spark)

    logger.info("Starting Read Dataframe via file types")

    # Read Dataframe from parquet file
    df = (spark.read
         .format("parquet")
         .option("path", "data/flight-time.parquet")
         .load())

    # Repartition
    logger.info("Num Partitions before: " + str(df.count()))
    df.groupBy(spark_partition_id()).count().show()

    partitionedDF = df.repartition(5)
    logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    # Write dataframe to Orc:
    (partitionedDF.write
     .format("avro")
     .mode("overwrite")
     .option("path", "dataSink/avro/")
     .save())

    # Write dataframe with repartition by column
    (df.write
        .format("json")
        .mode("overwrite")
        .option("path", "dataSink/json/")
        .partitionBy("OP_CARRIER", "ORIGIN")
        .option("maxRecordsPerFile", 10000)
        .save())