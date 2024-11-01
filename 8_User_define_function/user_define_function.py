import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import column, col, to_date, concat, udf, expr
from pyspark.sql.types import StringType

from utils.utils import get_spark_app_config
from utils.logger import Log4j

# Path for spark source folder (add because env in os do not set)
os.environ["JAVA_HOME"] = "D:\\anmv2\\Environment\\Jdk1.8"
# os.environ["HADOOP_HOME"] = "D:\\anmv2\\Environment\\hadoop-3.3.6"
# os.environ["SPARK_HOME"] = "D:\\anmv2\\Environment\\Spark\\spark-3.5.3-bin-hadoop3"

#  Function for using in transform
def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # config for logging
    logger = Log4j(spark)

    logger.info("Create and use User Define Function in Spark Dataframe")

    survey_df = (spark.read
                      .format("csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .option("path","data/survey.csv")
                      .load())

    # Method 1:
    # register for function coding:
    parse_gender_udf = udf(parse_gender, returnType=StringType())
    logger.info("Method 1 - Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]

    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    # Method 2:
    # register for SQL coding:
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Method 2 - Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)
