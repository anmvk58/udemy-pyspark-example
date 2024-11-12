from pyspark.sql import SparkSession

from utils.utils import get_spark_app_config
from utils.logger import Log4j


if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # config for logging
    logger = Log4j(spark)

    logger.info("Shuffle join in case 2 large DF, them must exchange key before join")

    spark.conf.set("spark.sql.shuffle.partitions", 3)
    logger.info("Set amount of shuffle partitions = 3 by using config spark.sql.shuffle.partitions")

    flight_time_df1 = (spark.read
                  .format("json")
                  .option("path", "data/d1/*.json")
                  .load())

    flight_time_df2 = (spark.read
                       .format("json")
                       .option("path", "data/d2/*.json")
                       .load())

    join_df = flight_time_df1.join(flight_time_df2, ['id'], "inner")
    join_df.collect()

    # For prevent this program finish to watch SparkUI
    input("press a key to stop...")


