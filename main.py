# This is a sample Python script.
from pyspark.sql import SparkSession
from lib import utils
from lib import transformations


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def hello_spark_without_conf_file(app_name):
    spark_session = SparkSession.builder.appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

    data_list = [("Ravi", 28), ("David", 45), ("Abdul", 37)]
    data_frame = spark_session.createDataFrame(data_list).toDF("Name", "Age")
    data_frame.show(n=3)
    spark_session.stop()


def hello_spark_with_conf_file():
    spark_session = utils.create_spark_session_from_config_file(config_file_location="config/spark.conf",
                                                                enable_hive_support=False)

    data_list = [("Ravi", 28), ("David", 45), ("Abdul", 37)]
    data_frame = spark_session.createDataFrame(data_list).toDF("Name", "Age")
    data_frame.show(n=3)

    utils.stop_spark_session(spark_session=spark_session)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # hello_spark_without_conf_file("Spark Program to demo use of file and database table based data sources and sinks")
    # hello_spark_with_conf_file()

    spark_session = utils.create_spark_session_from_config_file(config_file_location="config/spark.conf",
                                                                enable_hive_support=False)

    # transformations.analyse_log_files(spark_session=spark_session, log_file_path="data/apache_logs.txt")
    transformations.transform_survey_data_using_udf(spark_session=spark_session,data_file_path="data/survey.csv")
    utils.stop_spark_session(spark_session)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
