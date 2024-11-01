import os

from pyspark.sql import SparkSession

from sql_and_dataframe.list_anwser import *
from utils.utils import get_spark_app_config
from utils.logger import Log4j

# Path for spark source folder (add because env in os do not set)
os.environ["JAVA_HOME"] = "D:\\anmv2\\Environment\\Jdk1.8"
os.environ["HADOOP_HOME"] = "D:\\anmv2\\Environment\\hadoop-3.3.6"
os.environ["SPARK_HOME"] = "D:\\anmv2\\Environment\\Spark\\spark-3.5.3-bin-hadoop3"

# Append pyspark  to Python Path
# sys.path.append("D:\\anmv2\\Environment\\Spark\\spark-3.5.3-bin-hadoop3\\python")
# sys.path.append("D:\\anmv2\\Environment\\Spark\\spark-3.5.3-bin-hadoop3\\python\\lib\\py4j-0.10.9.7-src")

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # config for logging
    logger = Log4j(spark)

    logger.info("Starting HelloSpark")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("C:\\Users\\anmv2\\Downloads\\data_fire_department_2016.csv")

    # rename column to correct (remove blank space)
    df = df.withColumnsRenamed({
        "Call Number": "CallNumber",
        "Unit ID": "UnitID",
        "Incident Number": "IncidentNumber",
        "Call Date": "CallDate",
        "Watch Date": "WatchDate",
        "Call Final Disposition": "CallFinalDisposition",
        "Available DtTm": "AvailableDtTm",
        "Zipcode of Incident": "ZipCode",
        "Station Area": "StationArea",
        "Final Priority": "FinalPriority",
        "ALS Unit": "ALSUnit",
        "Call Type Group": "CallTypeGroup",
        "Unit sequence in call dispatch": "UnitSequenceInCallDispatch",
        "Fire Prevention District": "FirePreventionDistrict",
        "Supervisor District": "SupervisorDistrict",
        "Received DtTm": "ReceivedDtTm",
        "Entry DtTm": "EntryDtTm",
        "Dispatch DtTm": "DispatchDtTm",
        "Response DtTm": "ResponseDtTm",
        "On Scene DtTm": "OnSceneDtTm",
        "Transport DtTm": "TransportDtTm",
        "Hospital DtTm": "HospitalDtTm",
        "Original Priority": "OriginalPriority",
        "Number of Alarms": "NumberOfAlarms",
        "Unit Type": "UnitType",
        "Neighborhooods - Analysis Boundaries": "Neighborhooods"
    })

    # Correct data type for date column:
    df = df \
        .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy")) \
        .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy")) \
        .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a")) \
 \
    # df.printSchema()
    # df.show()
    # create template view for spark Sql
    df.createOrReplaceTempView("fire_service_calls_view")

    # question 1
    question_1(spark, df)

    # question 2
    # question_2(spark, df)

    # question 3
    # question_3(spark, df)

    # question 4
    # question_4(spark, df)

    # question 5
    # question_5(spark, df)

    # question 6
    # question_6(spark, df)

    # question 7
    # question_7(spark, df)

    # question 8
    # question_8(spark, df)

    # question 9
    # question_9(spark, df)

    # question 10
    # question_10(spark, df)

    logger.info("Finished HelloSpark")
    spark.stop()
