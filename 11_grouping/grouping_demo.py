import os

from pyspark.sql import SparkSession
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

    logger.info("Demo Grouping Data by transform some columns")

    invoice_df = (spark.read
                  .format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .option("path", "data/invoices.csv")
                  .load())

    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValue")

    exSummary_df = invoice_df.withColumn("InvoiceDate", f.to_date("InvoiceDate", "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear("InvoiceDate")) \
        .groupBy("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)

    # exSummary_df.coalesce(1) \
    #     .write \
    #     .format("parquet") \
    #     .mode("overwrite") \
    #     .save("output")

    exSummary_df.sort("Country", "WeekNumber").show()
