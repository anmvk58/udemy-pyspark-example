import os

from pyspark.sql import SparkSession

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

    logger.info("Example for join 2 dataframe")

    orders_list = [("01", "01", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")

    # Method 1: condition (IDE warning)
    order_df.join(other=product_renamed_df, on=order_df.prod_id == product_renamed_df.prod_id, how="inner") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .show()

    # Method 2: List column (2 df have same name on join column)
    order_df.join(other=product_renamed_df, on=['prod_id'], how="inner") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .show()

    # DEPRECATED !!!
    # Method 3: Use dict to define list columns join from 2 df
    # list_col_join = {
    #     "prod_id": "prod_id",
    #     "order_id": "prod_id"
    # }
    # order_df.join(other=product_renamed_df, on=list_col_join, how="inner") \
    #     .drop(product_renamed_df.prod_id) \
    #     .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
    #     .show()

    # Join on multiple colums:
    # [df.name == df3.name, df.age == df3.age]
    condition_join = [
        order_df.prod_id == product_renamed_df.prod_id,
        order_df.order_id == product_renamed_df.prod_id
    ]
    order_df.join(other=product_renamed_df, on=condition_join, how="inner") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .show()