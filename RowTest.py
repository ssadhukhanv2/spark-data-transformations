from datetime import date
from unittest import TestCase

from pyspark.sql.types import StructType, StructField, StringType, Row

from lib import utils


class RowDemoTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_session = utils.create_spark_session_from_config_file("config/spark.conf")
        my_schema = StructType([StructField("ID", StringType()), StructField("EventDate", StringType())])

        my_rows = [Row("123", "04/05/2020"), Row("124", "04/05/2020"), Row("125", "04/05/2020"),
                   Row("126", "04/05/2020")]
        my_rdd = cls.spark_session.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark_session.createDataFrame(my_rdd, my_schema)

    def test_data_type(self):

        # my_df is just a reference to the data sitting in the driver, so we need to
        # explicitly call the .collect() method to collect the rows and
        # bring it to the driver so that we can assert them

        rows = utils.to_date_df(dataframe=self.my_df, date_format="M/d/y", column_name="EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_date_value(self):
        rows = utils.to_date_df(dataframe=self.my_df, date_format="M/d/y", column_name="EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))
