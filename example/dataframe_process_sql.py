import os

from babel.util import distinct
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
            .option("inferSchema", "true")\
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
        "Zipcode of Incident": "Zipcode",
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
        "Neighborhooods - Analysis Boundaries": "NeighborhooodsAnalysisBoundaries"
    })

    # Correct data type for date column:
    df = df \
        .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy")) \
        .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy")) \
        .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a")) \

    df.printSchema()

    # Q1:How many distinct types of calls were made to the Fire Department?
    # method 1 using spark sql:
    df.createOrReplaceTempView("fire_service_calls_view")
    q1_sql_df = spark.sql("""
        select 
            count(distinct CallTypeGroup) as distinct_call_type_count 
        from
            fire_service_calls_view
        where 
            CallTypeGroup is not null
    """)
    q1_sql_df.show()

    # method 2 using dataframe
    df = df.where(df.CallTypeGroup.isNotNull()) \
        .select("CallTypeGroup") \
        .distinct() \

    print('number of distinct CallTypeGroup = ' + df.count())

    spark.stop()

