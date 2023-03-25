from pyspark.sql.functions import regexp_extract, substring_index, udf, expr
from pyspark.sql.types import StringType

from lib import utils


def analyse_log_files(spark_session, log_file_path="data/apache.logs"):
    file_df = spark_session.read.text(log_file_path)
    file_df.printSchema()
    print(file_df.count())

    logs_regex = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    # applying schema to the log entries using regex and extracting useful data as columns to our dataframe
    # logs_df=file_df.select(*[regexp_extract('value',logs_regex,i).alias("my_field") for i in range(1,12)])
    # logs_df.show()

    # all the columns are not particularly interesting enough to be used
    # so we only extract the 1st, 4th, 6th and 10th columns
    # and extract into our dataframes
    logs_df = file_df.select(regexp_extract('value', logs_regex, 1).alias("ip"),
                             regexp_extract('value', logs_regex, 4).alias("date"),
                             regexp_extract('value', logs_regex, 6).alias("date"),
                             regexp_extract('value', logs_regex, 10).alias('referrer'))

    logs_grouped_by_referrer_df = logs_df \
        .where("trim(referrer)!='-'") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count()

    logs_grouped_by_referrer_df.show()
    utils.stop_spark_session(spark_session)


def transform_survey_data_using_udf(spark_session, data_file_path="data/survey.csv"):
    survey_df = spark_session.read.format("csv") \
        .option("path", data_file_path) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load()
    survey_df.select("Age","Gender","Country","State").show(n=5)

    # Creating an UDF from utils.parse_gender so that it may be used within the .withColumn transformation
    parse_gender_udf = udf(utils.parse_gender, returnType=StringType())

    print("Catalog Entry:")
    [print(r) for r in spark_session.catalog.listFunctions() if "parse_gender_udf" in r.name]

    survey_df_with_parsed_gender = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df_with_parsed_gender.select("Age","Gender","Country","State").show(n=5)

    # Registering the UDF created from utils.parse_gender, so it is available in the catalog and may be used in sql
    # expression
    spark_session.udf.register("parse_gender_udf", utils.parse_gender, StringType())
    print("Catalog Entry:")
    [print(r) for r in spark_session.catalog.listFunctions() if "parse_gender_udf" in r.name]

    survey_df_with_parsed_gender_with_sql_expr=survey_df.withColumn("Gender",expr("parse_gender_udf(Gender)"))
    survey_df_with_parsed_gender_with_sql_expr.select("Age","Gender","Country","State").show(n=5)
