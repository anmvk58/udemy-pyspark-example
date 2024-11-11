import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, Row
from pyspark.sql.functions import col, to_date

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

    logger.info("Example for using Rows object")

    # Create a schema
    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

    # Create List Rows Objects
    my_rows = [
        Row("123", "04/05/2020"),
        Row("124", "4/5/2020"),
        Row("125", "04/5/2020"),
        Row("126", "4/05/2020"),
    ]

    # Create Rdd from Rows
    # parallelize(data, numSlices):  => numSlices = number of partitions
    # phương thức được dùng để tạo một RDD (Resilient Distributed Dataset) từ một danh sách dữ liệu (list)
    # hoặc bất kỳ một collection nào có thể lặp lại (iterable) trong Python
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()

    # Convert to_date function:
    converted_df = my_df.withColumn("EventDate", to_date(col("EventDate"), "M/d/y"))
    converted_df.printSchema()
    converted_df.show()
