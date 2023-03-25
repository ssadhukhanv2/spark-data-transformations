## Important Links
* [Spark Documentation](https://spark.apache.org/docs/latest/index.html)
* [Overview of the spark cluster](https://spark.apache.org/docs/latest/cluster-overview.html)

## Spark Setup in Windows

* Install JDK11 from [here](https://jdk.java.net/archive/), set it as JAVA_HOME and add `%JAVA_HOME%\bin` to the system path.
* Install [winutils](https://github.com/steveloughran/winutils) for Hadoop. 
  * [Latest version of winutils]((https://github.com/cdarlint/winutils)) install from here 
  * Download or clone the repo and copy the folder of the latest version available(in our case hadoop-3.2.2) to a location.
  * Set this has HADOOP_HOME to this folder and add `%HADOOP_HOME%\bin` to the system path
* Install Spark 
  * Download Spark from [here](https://spark.apache.org/downloads.html) and extract it in a folder location
  * Set `SPARK_HOME` variable and add `%SPARK_HOME%\bin` to the system path variable
* Install pyspark in the venv using pip
  * Activate the virtual environment `.\venv\Scripts\activate`
  * Install the pyspark package `pip install pyspark`
* Incase multiple versions of python installed on a machine, ensure Python 3.10 is installed and configure the following environment variables
  * Set `PYTHONPATH` to `C:\Users\subhr\Softwares\spark-3.3.2-bin-hadoop3\python;C:\Users\subhr\Softwares\spark-3.3.2-bin-hadoop3\python\lib\py4j-0.10.9.5-src.zip`
  * Set `PYSPARK_PYTHON` to `C:\Program Files\Python310\python.exe` 
    * Without this `PYSPARK_PYTHON` environment variable, running code form pycharm doesn't work, but with this environment variable pyspark from commandline doesn't work `%SPARK_HOME%\bin\pyspark --version`. So while using the commandline change the environment variable name to `PYSPARK_PYTHON_XXXXX`

## Project Setup 

* [**Course GitHub Link**](https://github.com/LearningJournal/Spark-Programming-In-Python/tree/master/01-HelloSpark)
* [**SparkBy{Examples} Link**](https://sparkbyexamples.com/spark/how-to-create-an-rdd-using-parallelize/)
* **Spark UI Available at [http://localhost:4040/](http://localhost:4040/)**

* [**Anaconda**](https://www.youtube.com/watch?v=MUZtVEDKXsk&t=625s&ab_channel=PythonSimplified): Install Anaconda and use it as the package manager for creating the project. 
  * Launch the Anaconda shell activate the hello-spark environment `conda activate hello-spark` 
  * Install Pyspark using conda `conda install -c conda-forge pyspark`
  * Install Pytest using conda `conda install -c anaconda pytest`
  
  [Anaconda difference with pip](https://www.reddit.com/r/Python/comments/w564g0/can_anyone_explain_the_differences_of_conda_vs_pip/)

* [Using spark-shell in client mode locally](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162098#overview) Using the spark-shell in client mode. UI launches in [http://localhost:4040/executors/](http://localhost:4040/executors/) 

      ## check spark version
      %SPARK_HOME%\bin\pyspark
      
      ## Launch sparkshell
      %SPARK_HOME%\bin\pyspark --master local[3] --driver-memory 2G

*  [Create a multinode spark cluster in GCP](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20218636#overview)
* [Connect to the multi node spark cluster using `spark-shell` and `Zeppelin`](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162104#overview) 
  * Lauch pyspark: `pyspark --master yarn --driver-memory 1G --executor-memory 500M --num-executors 2 --executor-cores 1` 
  * Spark History Server: All applications which have completed their execution on the spark are displayed here.
  * The application which are currently running applications may be displayed within incomplete applications under spark history server. But to get a view of both incomplete and the inactive(completed) application you can view under the resource manager.
* [Submitting jobs using the spark-submit](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162116#overview) [Need to practice]
  
      spark-submit --master yarn --deploy-mode cluster pi.py 100


* To use formats like AVRO we need to add the below jars to the spark in the `spark-defaults.conf` file. Read more [Apache AVRO Datasource Guide](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)

      spark.jars.packages                org.apache.spark:spark-avro_2.12:3.3.2

## [Spark Basic Concepts](Readme_spark_basics.md) 

## Working with File based Data Sources and Sink

* parquet is the default recommended file format for spark
* avro is the default recommended file format for kafka
* [**Spark Data Sources and Sink**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20434107#overview) Using spark apis we may read data from an external or internal data source, process them using spark and then write data to an external or internal data sink.
  * **Spark Data Source** A data processing engine like Spark may read data from a source, this is called a spark data source. Spark Data Sources may be categorized into two groups:
    1. **External Data Sources:** Data Sources that resides outside our datalake are called external data sources. For example the data may reside in Oracle Database, an SQL server, an application server(like application logs) etc.
       * **JDBC Data Sources**: Oracle, PostgreSQL, SQL Server etc
       * **NoSQL Data Sources**: Mongo, Cassandra
       * **Cloud Data Warehouses**: Snowflake, Redshift
       * **Stream Integrators**: Kafka Kinesis
       
       **Consuming Data from External Datasources:** To process data from the external datasources using spark we need to read the data and create a Dataframe or a Dataset. This may be done using two approaches:
       * **Approach 1: *Consume data from External Sources using Data Integration Tools nd load into Internal datasource as files*** Bring Data from the external source to the datalake and store them in the datalake's distributed store. We may use **`data integration tools`** like **[HVR](https://fivetran.com/docs/hvr5/introduction), [Informatica](https://www.informatica.com/in/), [Glue](https://aws.amazon.com/glue/), [Talend](https://www.talend.com/knowledge-center/), [Kafka](https://kafka.apache.org/)** *This approach is preferred for batch processing requirements* 
         * This approach is mostly preferred in real life scenarios even though we can directly connect to the sources because spark was initially designed as a data processing tool not a data ingestion tool. So using a data ingestion/integration tool like the one's mentioned above brings modularity, load balancing, security and flexibility while injesting the data.  
       * **Approach 2: *Use Spark Datasource API to directly read Data from External Sources*** Use spark datasource API to directly connect with these external systems. *This approach is preferred for stream processing requirements*
    2. **Internal Data Sources:** The internal datasource would be our distributed data storage, it could be HDFS or Cloud Based Storage like HDFS, S3, Azure Blob, Google Cloud etc. So the data that we may have injested using the data integration tools would be stored as files within our intenal data store and we may use spark to read and process that data. 
       * **File Sources - `CSV, JSON, Parquet, AVRO or Plain Text`** The data may be stored within our internal data source as CSV, JSON, Parquet, AVRO or Plain Text data files. 
       * **Non-file sources - `SQL Tables or Delta Lake`** The data may be stored within our internal data source as SQL Tables or Delta Lake. These are formats backed by data files but for these formats addional metadata needs to be stored outside a datafile. 
  * **Data Sink:** Data Sink is the final destination of the processed data. Similar to external and internal datasource we may have internal or external data sink.

* **[Spark DataFrameReader:](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20434113#overview)** Spark DataFrameReader allows us to read data from external storage systems like file-systems, keyvalue stores, etc [Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html#pyspark.sql.DataFrameReader). 
* [Reading .csv, .parquet and .json files](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20434113#notes) 
    * We can use `spark_session.read` to get the dataframereader on any of the dataframes and use the methods to customize the reader. For example the following reads a csv files from a given files and loads it into a data frame

          data_frame_csv = spark_session.read.format("csv") \
          .option("header", "true") \
          .option("path", files_path) \
          .option("mode", "FAILFAST") \
          .load()

    * `.format():` a lot of community formats like JSON, AVRO, CSV, Cassandra, MongoDB, AVRO, XML, HBase, Redshift are supported by .format() method
    * `.option(path,files_path):` files_path is the path to the directory where the files are present,
      * path may be a file name like "data/flight-time.csv"
      * path may be a folder name like "data/" , this will pick up all files within the directory. *May cause confusion so while using this approach assume that all files within the directory will be picked up expecting a specific format of files to be picked up based on .format specified*
      * path may be a pattern like "data/flight*.csv" , this will pick up all files matching the pattern
    * `.option("mode", read_mode):` mode is used to specify the read mode. There are 3 read modes in spark
      * `PERMISSIVE` -> the PERMISSIVE read mode inserts a row with all null columns whenever it encounters a corrupted record and places the corrupted record in a string column called _corrupt_record
      * `DROPMALFORMED` -> the DROPMALFORMED read mode simple ignores the malformed record, thus drops it while reading into dataframe
      * `FAILFAST` -> the FAILFAST read mode raises an exception and terminates immediately whenever a corrupted record is encountered
    * `.schema:` schema may be implicit, explicit or inferschema
      Infer Schema -> We may choose not to provide an explicit schema for our dataset dataframereader infers the schema
      Explicit -> if we choose not to provide an explicit schema for our dataset dataframereader infers the schema
      Implicit -> Some of the data sources like AVRO and PARQUET comes with an well defined schema, for these we don't need to define a schema
    * `.load():` method is called to create the dataframe


* [**Defining Spark Dataframe Schema**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20434117#notes) Spark acts like a compiler by converting highlevel API(like Dataframe and Datasets) code into low level code(RDD). Each compiler needs it own data type to optimize the code execution so spark also maintains its own set of datatypes [spark data types docs](https://spark.apache.org/docs/latest/sql-ref-datatypes.html). The schema may be automatically inferred, or we may pass a ddl or programmatically pass  Spark Dataframe schema.
   * Programmatically: We use StructType and StructField 
   * Using DDL String: We may also pass ddl strings as schema. FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT



* **[Spark DataFrameWriter:](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20434121#notes)** Spark DataFrameWriter allows us to write files to a sink. We can get the writer using DataFrame.write() method and we have multiple options to format and restructure our data while writing.
    * `.format(format_name)`: this allows us to specify the format of the file that we want to write. Built in formats include CSV, JSON, Parquet, ORC, JDBC. Community formats include Cassandra, MongoDB, AVRO, XML, HBase, Redshift, etc
    * `.option("path","data/flights")`: we may use this to specify the target of our data frame. This would generally be a directory location
    * `.mode(saveMode)` saveMode defines what will happen if spark finds an existing dataset in the specific location. There are 4 save modes in spark:
      * append -> creates new file without touching the existing data in the given location
      * overwrite -> removes existing data files and creates new files 
      * errorIfExists -> throws an error if there are data in the given location
      * ignore -> ignore will write the data if the target directory is empty and do nothing if files exist in the given location

* **Spark File Layout**: While using DataFrameWriter we can control the layout of our files. We can control the Number of files and File Size, Partitions and Buckets, Storing Sorted Data.
* **Controlling the Number of files and File Size**: Typically the number a file is generated for every partition within the dataframe, however we can tweak the behaviour by repartitoning the data based on our choices. 
  * **Repartitioning our data**: We can repartition our data using the below methods:
    * `Dataframe.repartition(n)`: Here n determines the number of partition and therefore the number of output files. This would be a blind repartition.
    * `DataframeWriter.partitionBy(col1,col2)`: This method partitions our data based on a key column, we may use use a single column or a composite column(like inour example). This method is suitable to break our data logically and also help us improve our spark sql performance using the partition pruning technique.
        *Reshuffling the data may take some time* 
    * `maxRecordsPerFile`: This is an option is used to control the file size by defining an upper limit on the max number of records per file. Helps us protect from creating large inefficient.
      * Basically creates a directory structure like
          
            |---col1-val1
                   |------col2-val1
                             |-file1.json [number of records<=maxRecordsPerFile]
                             |-file2.json [number of records<=maxRecordsPerFile]
                   |------col2-val1
                             |-file1.json [number of records<=maxRecordsPerFile]
                             |-file2.json [number of records<=maxRecordsPerFile]
                             |-file3.json [number of records<=maxRecordsPerFile]
    * `DataframeWriter.buckeyBy(col1,col2)` This method helps us partition our data into a fixed number of buckets and this is known as bucketing. This method is only allowed in Spark managed tables
    * `.sortBy` This method is commonly used with bucketBy and use to define a sort order for the data within the bucket.


## Working with Databases and Tables
* [**Spark Database and Tables:**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20434129#notes) Spark is not only a data processing engine, we can also create databases within spark. We can create Tables and Views within our spark database:
  * **Views:** Has same concept as views of sql with architecture of spark tables
  * **Table**: Table has Table Data and Table Metadata
    * **Table Data**: This is stored in the distributed storage. By default, this would be a Parquet file.
    * **Table Metadata**: The Table metadata is stored within a metastore called catalog, which  is a central place which holds information about the  schema, table name, database name, column names, partitions and the physical location where the datafiles are resides. 
        
      By default, spark comes with an in-memory catalog which is mentioned per spark session, however this information goes away when session ends. But we may need a persistent storage for the catalog, in such scenarios we may use a hive data store.
    * **Types of Spark Tables**
      * **Managed Tables**: Here spark manages both the table data and the table metadata. 
        * When we create or write data into a managed table the catalog is updated and the datafiles are stored in a predefined spark `location spark.sql.warehouse.dir`
        * If we drop a managed table, it drops the table as well as the metadata
        * With managed table we are able to perform operations such as bucketing etc.
      * **Unmanaged Tables(External Tables)**: In case of unmanaged table spark the table metadata is stored in the catalog metastore but the data files have to be explicitly specified by the user as "data_file_location". 
        * *Unmanaged tables gives us the flexibility to store data in some external location but allow processing the data within spark with explicitly copying the data within spark* 
        *  If we drop an unmanaged table, it drops the metadata, the data files in teh external location remains as is.
        * Used for reusing existing data within spark.
  * **Advantages of using Spark Database Tables over simply processing data using raw data files** Spark Tables can be accessed using the JDBC/ODBC connector, so if we store the data in spark database, this will allow other tools like PowerBi and Table to connect to the spark database and allow accessing them. 
     
