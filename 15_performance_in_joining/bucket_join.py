from pyspark.sql import SparkSession

from utils.utils import get_spark_app_config
from utils.logger import Log4j

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    # config for logging
    logger = Log4j(spark)

    logger.info("Repartition before join can better !")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    logger.info("Force disable Broadcast Join by using config spark.sql.autoBroadcastJoinThreshold")

    df1 = (spark.read
           .format("json")
           .option("path", "data/d1/*.json")
           .load())

    df2 = (spark.read
           .format("json")
           .option("path", "data/d2/*.json")
           .load())

    # Create DB
    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")

    # Repartition by key join and write bucket equal with executors
    df1.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data1")

    df2.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data2")

    df3 = spark.read.table("MY_DB.flight_data1")
    df4 = spark.read.table("MY_DB.flight_data2")

    join_df = df3.join(df4, ["id"], "inner")
    join_df.show()

    input("press a key to stop...")
