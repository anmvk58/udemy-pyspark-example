import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

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

    logger.info("When you want to calculate data base on 1 windows data")

    summary_df = (spark.read
                  .format("parquet")
                  .option("path", "data/summary.parquet")
                  .load())

    summary_df = summary_df.withColumn("InvoiceValue", col("InvoiceValue").cast(DecimalType(38, 10)))
    summary_df.printSchema()

    # IMPORTANT: define windows we want to calculate
    # rowFrame: Áp dụng tính toán dựa trên các chỉ số hàng cụ thể trong cửa sổ.
    # rangeFrame: Áp dụng tính toán dựa trên các giá trị trong cột được sắp xếp (theo ORDER BY).
    # unboundedPreceding: Bắt đầu từ hàng đầu tiên trong cửa sổ.
    # unboundedFollowing: Kết thúc ở hàng cuối cùng trong cửa sổ.
    # currentRow: Kết thúc ở hàng hiện tại.

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    result_df = summary_df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window))

    result_df.show()
