from pyspark.sql.functions import regexp_extract, substring_index, udf, expr, monotonically_increasing_id, col, to_date, \
    when
from pyspark.sql.types import StringType, IntegerType

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
    survey_df.select("Age", "Gender", "Country", "State").show(n=5)

    # Creating an UDF from utils.parse_gender so that it may be used within the .withColumn transformation
    parse_gender_udf = udf(utils.parse_gender, returnType=StringType())

    print("Catalog Entry:")
    [print(r) for r in spark_session.catalog.listFunctions() if "parse_gender_udf" in r.name]

    survey_df_with_parsed_gender = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df_with_parsed_gender.select("Age", "Gender", "Country", "State").show(n=5)

    # Registering the UDF created from utils.parse_gender, so it is available in the catalog and may be used in sql
    # expression
    spark_session.udf.register("parse_gender_udf", utils.parse_gender, StringType())
    print("Catalog Entry:")
    [print(r) for r in spark_session.catalog.listFunctions() if "parse_gender_udf" in r.name]

    survey_df_with_parsed_gender_with_sql_expr = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df_with_parsed_gender_with_sql_expr.select("Age", "Gender", "Country", "State").show(n=5)


def miscellaneous_dataframe_transformations(spark_session):
    # manually create a dataframe
    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark_session.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

    # add a new column
    # add a monotonically increasing id as "id" column
    # monotonically_increasing_id() generates an ID that is
    # guaranteed to be unique monotonically increasing but not consecutive
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.show()

    # Years have been given in both 2 digit and 4 digit format. Infer and generate the years correctly
    # here we used case as sql statement
    df2 = df1.withColumn("year", expr("""
    case when year<21 then year+2000
    when year<100 then year+1900
    else year
    end
    """))
    df2.show()

    # after previous transformation some previous years seems to be cast as decimals like 1981.0, 1963.0
    # this happens because spark converts the string into a float and does the addition, as a result the addtional .0
    # so to eliminate this we may want to cast our data frame column
    # there are two ways to casting data
    #   1. Inline casting: Cast at the place where you need. This doesn't modify the schema of the dataframe.
    #   2. Change the schema: Changes the schema

    # Inline casting:
    df3 = df1.withColumn("year", expr("""
    case when year<21 then cast(year as int)+2000
    when year<100 then cast(year as int)+1900
    else year
    end
    """))
    df3.show()
    df3.printSchema()  # As we can see that all the columns are still strings

    # Change the schema using Explicit Casting
    df5 = df1.withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType()))

    # using case expressions for data manipulation
    df6 = df5.withColumn("year", expr("""
    case when year<21 then cast(year as int)+2000
    when year<100 then cast(year as int)+1900
    else year
    end
    """))
    df6.show()
    df6.printSchema()

    # using spark functions for implementing case
    df7 = df5.withColumn("year", \
        when(col("year") < 21, col("year") + 2000) \
        .when(col("year") < 100, col("year") + 1900) \
        .otherwise(col("year")))
    df7.show()

    # combine columns
    df8 = df7.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))
    df8.show()

    # drop unnecessary columns
    df9 = df7.withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"), 'd/M/y')) \
        .drop("day", "month", "year")

    # there are two rows with name "Abdul" so we want to remove duplicates
    # here we remove duplicates from that has same name or same dob
    df10 = df9.dropDuplicates(["name", "dob"])
    df10.show()

    ## sort the rows by dob
    df10_sorted_ascending = df10.sort("dob")
    df10_sorted_ascending.show()

    # This doesn't seem to work, check later
    df10_sorted_descending = df10.sort(expr("dob desc"))
    df10_sorted_descending.show()
