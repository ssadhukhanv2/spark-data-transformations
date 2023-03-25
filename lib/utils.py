import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import re


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


def parse_gender(gender):
    # This will be registered as a User Defined Function(UDF), given the value in the gender column
    # it provides a standardized format to the gender

    # Returns "Female" if female_pattern matches
    # Returns "Male" if male_pattern matches

    # "^f$|f.m|w.m" contains patterns to infer the female gender, the patterns are combined using or(|)
    #   ^f$ -> matches words starting and ending with "f". This means it will match "f"
    #   f.m -> matches words with having single character between f and m like "fem" in "female"
    #   w.m -> matches words with having single character between w and m like "wom" in "woman", "women"
    female_pattern = r"^f$|f.m|w.m"
    # same with as female pattern
    male_pattern = r"^m$|ma|m.l"

    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"
