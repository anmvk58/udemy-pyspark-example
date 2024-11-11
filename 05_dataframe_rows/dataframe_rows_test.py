from datetime import date
from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import Row, StructType, StructField, StringType

from utils.utils import get_spark_app_config

class RowDemoTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        conf = get_spark_app_config()
        cls.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [
            Row("123", "04/05/2020"),
            Row("124", "4/5/2020"),
            Row("125", "04/5/2020"),
            Row("126", "4/05/2020")
        ]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)


    def test_data_type(self):
        rows = self.my_df.withColumn("EventDate", to_date(col("EventDate"), "M/d/y")).collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_data_value(self):
        rows = self.my_df.withColumn("EventDate", to_date(col("EventDate"), "M/d/y")).collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))