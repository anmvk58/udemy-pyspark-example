import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, when, expr
from pyspark.sql.types import  IntegerType

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

    logger.info("Misc in processing columns")

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")   # 1981
                ]

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.show()

    # apply some misc
    final_df = (raw_df.withColumn("id", monotonically_increasing_id())
                .withColumn("day", col("day").cast(IntegerType()))
                .withColumn("month", col("month").cast(IntegerType()))
                .withColumn("year", col("year").cast(IntegerType()))
                .withColumn("year", when(col("year") < 20, col("year") + 2000)
                            .when(col("year") < 100, col("year") + 1900)
                            .otherwise(col("year")))
                .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
                .dropDuplicates(["name", "dob"])
                .drop("day", "month", "year")
                # .sort(expr("dob desc"))  # not work
                .sort(col("dob").desc())
                )

    final_df.show()