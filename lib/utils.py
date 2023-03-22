import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date


def read_spark_config(config_file_location):
    # Here we are reading the config/spark.conf file to configure the SparkConf
    # returns SparkConf object
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read(config_file_location)
    for key, val in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def create_spark_session_from_config_file(config_file_location, enable_hive_support=False):
    spark_conf = read_spark_config(config_file_location)
    spark_session = None
    if enable_hive_support:
        # In this scenario we are enabling Hive Support for using the session with Spark Tables
        spark_session = SparkSession.builder.config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        spark_session = SparkSession.builder.config(conf=spark_conf) \
            .getOrCreate()
    return spark_session


def stop_spark_session(spark_session):
    spark_session.stop()


def to_date_df(dataframe, date_format, column_name):
    return dataframe.withColumn(column_name, to_date(column_name, date_format))
