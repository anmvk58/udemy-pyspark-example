import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

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

    # Make Schema for CSV and Json Type
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""


    # CSV Reader
    flight_df_csv = (spark.read
                     .format("csv")
                     .option("header", "true")
                     .option("mode", "FAILFAST")
                     .option("dateFormat", "M/d/y")
                     .option("path", "data/flight-time.csv")
                     .schema(flightSchemaStruct)
                     .load())

    flight_df_csv.show(5)
    logger.info("CSV Schema:" + flight_df_csv.schema.simpleString())
    flight_df_csv.printSchema()


    # JSON Reader
    flight_df_json = (spark.read
                      .format("json")
                      .option("path", "data/flight-time.json")
                      .option("dateFormat", "M/d/y")
                      .schema(flightSchemaDDL)
                      .load())

    flight_df_json.show(5)
    logger.info("JSON Schema:" + flight_df_json.schema.simpleString())
    flight_df_json.printSchema()


    # PARQUET Reader
    flight_df_parquet = (spark.read
                         .format("parquet")
                         .option("path", "data/flight-time.parquet")
                         .load())

    flight_df_parquet.show(5)
    logger.info("PARQUET Schema:" + flight_df_parquet.schema.simpleString())
    flight_df_parquet.printSchema()