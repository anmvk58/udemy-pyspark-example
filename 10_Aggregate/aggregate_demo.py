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

    logger.info("Demo Aggregate data")

    invoice_df = (spark.read
                  .format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .option("path", "data/invoices.csv")
                  .load())

    # Method using function
    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")).show()

    # Or using SelectExpr
    invoice_df.selectExpr(
        "count(1) as `Count 1`",
        "count(StockCode) as `Count Field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    # Method using Spark SQL
    invoice_df.createOrReplaceTempView("sales")

    summary_sql = spark.sql("""
        SELECT
            Country,
            InvoiceNo,
            sum(Quantity) as TotalQuantity,
            round(sum(Quantity*UnitPrice),2) as InvoiceValue
        FROM
            sales
        GROUP BY
            Country, InvoiceNo 
    """)

    summary_sql.show()

    # Using Function to aggregate
    summary_sql_func = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity*UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity*UnitPrice), 2) as InvoiceValueExpr")
             )

    summary_sql_func.show()
