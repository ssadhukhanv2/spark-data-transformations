## Important Links
* [Spark Documentation](https://spark.apache.org/docs/latest/index.html)
* [Overview of the spark cluster](https://spark.apache.org/docs/latest/cluster-overview.html)
* [**Course GitHub Link**](https://github.com/LearningJournal/Spark-Programming-In-Python/tree/master/01-HelloSpark)

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

## [Spark Core Concepts](https://spark.apache.org/docs/latest/cluster-overview.html#glossary) 
* Application, Application Jar, Driver Program, Cluster Manager, Deployment Mode, Work Node, Executor, Task, Job, Stage
* [Spark Master Urls](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)

## Spark Execution Model and architecture
* [**Spark Processing Model**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162052#overview) 
  * **Driver** When a client submits a job to the spark cluster, it creates a master process for the application. This master process is called the Driver in Spark terminology. It in turn requests executors to execute the job.
  * **Executor** This master process or driver then creates a bunch of slave process to execute and complete the job. These slave processes are known as executors.
  
  *Each job or spark application has it's own set driver and set of executors and is independent of other job/apps driver and set of executors*

* [**Spark Execution Modes**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162090#overview): Spark Programs may be executed in one of the following ways:
  * **Client Mode** Used with interactive spark clients Example- `Jupyter Notebook, PySpark Shell, etc.`. 
    * Here the driver resides within the local machine connects to a cluster using the cluster manager like YARN, Kubernetes, Mesos etc to spin up the executors which runs the program. 
    *  Quitting the interactive client or logging off results in the death of the driver which in turn results in the death of the executors as well. 
    * Spark interactive client is more suited for interactive work so the client mode is not suitable for long-running queries.  
  * **Cluster Mode** In cluster mode the executor and driver both run on the spark cluster. Using the `spark-submit` commandline utility we can submit spark application to a cluster. Example `spark-submit, Databricks, Notebook, REST API`
    * If we submit a job to the spark cluster using cluster mode, even if we log off from our client machine, the job still runs in the cluster. 
    * This mode is more suited for long-running jobs/queries.

* **Spark Cluster Managers:** Spark Cluster managers may be configured to work with a variety of different clusters managed by a variety of different cluster managers. 
  1. **local[n]** -> This is the local cluster manager. This uses the local cluster setup with n number of threads. *If n=1 or no n is specified, then there will be a single Driver API, in case of n>1, there will be 1 Driver API and n-1 Executor API for the application.*
  2. YARN
  3. Kubernetes
  4. Mesos
  5. Standalone


* [**Spark Session**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20192566#overview) Spark session is basically the entrypoint for Programming Spark. 
   * Created using the SparkSession.builder(). 
   * There should be a single driver per application
   * [**Configuring Spark Session:**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20192568#overview) Spark Session may be configured in 1 of the following 3 ways:
     1. Environment Variables (Lowest Precedence)
     2. SPARK_HOME\conf\spark-defaults.conf
     3. spark-submit command line options
     4. SparkConf Object (Highest Precedence)
     
     *All these configs are merged together with SparkConf Object having the highest precedence and Environment Variables having the lowest precedence*

     * **Spark Context** Represents connection to a Spark Cluster. 
       * This may be used to get the SparkConf object

     * **SparkConf** This is used to get or set various Spark parameters as key value pairs.
       * This may also be used to retrieve the current configuration within the spark session 
* [**Spark Data Frame:**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20192574#overview) Spark Data Frame is a two-dimensional table like datastructure that is inspired by Pandas Dataframe
    * They are a distributed table with named columns and well defined schemas i.e each column has a well-defined data type
    * Similar to Database Table and supports similar operations. 
      * .option("header", "true") infers the column names based on the table headers
      * .option("inferSchema", "true") makes an intelligent guess about the datatype of the columns

* [**Spark Data Frame partitions and executors**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20192576#overview) Say we are to load a file stored across multiple partions within a distributed file system like HDFS into a spark dataframe. Spark knows which partions the data resides, and it creates a logical in memory structure called the datafarme. 
  * This inmemory datastructure has not loaded the data yet, it creates the structure with enough information so that the data can be loaded from multiple partitons, we can think of frame containing multiple other small frames(each logically representing a partition).
  * The driver then requests the cluster manager to spin up the executor containers(based on the configuration) which are jvm processes within the cluster and distributes the frames across the different executors. These executors load the actual datafiles in memory and execute the job. 
  * Each executor is having its own set of CPU cores and memory and each CPU core will have its own data partition to work on.
  * Spark intelligently tries to optimize the bandwidth for loading the file partitions from the distributed storage by loading partions to executors which are nearest to them. 
* [**Spark Transformation**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20192578#overview) Spark Dataframe is a distributed datastructure but to a spark developer it looks like a single table and supports multiple transformation. Spark Dataframes are immutable, but we can give instruction to the driver for doing the transformation and the driver takes care of how the transformation can be achieved with the help of executors. These instructions to the driver are called transformation. Example-select, filter, groupby etc.
  * Each spark operation goes to the spark session.
  * Each Step is a dataframe which is the outcome of the intermediate transformation.
  * For example- 
    * *df_survey is the outcome of `utils.load_survey_df(spark, "data\survey.csv")` transformation*
    * *df_survey_filtered is the outcome of the `select` transformation*
    * *df_survey_grouped is the outcome of the groupBy transformation*
    
          df_survey = utils.load_survey_df(spark, "data\survey.csv")
          df_survey_filtered=df_survey.where("Age<40")\
            .select("Age","Gender","Country","State")
          df_survey_grouped=df_survey_filtered \
            .groupBy("Country")
          df_survey_grouped.count().show()
  * Spark Data Processing is basically about creating a [DAG(Distributed Acyclic Graph)](https://www.projectpro.io/recipes/what-is-dag-apache-spark#:~:text=DAG%20or%20Directed%20Acyclic%20Graph,to%20be%20applied%20on%20RDD.) of operations. For the above code the DAG may look like `read->where->select->groupby-count->show`
    * Spark Operations are of two types:
      * **Transformations:** Any operation which reads a dataframe and converts it to another data frame without modifying the actual dataframe is called a transformation. For example, the `where` clause transformation, it basically works on the input dataframe and produces an output dataframe but the input dataframe remains unaltered. Transformations are further classified into narrow dependency transformation and wide dependency transformation.
        * **Narrow Dependency Transformation** These are transformation that may be applied on a single partition and yet produce a valid result. These are transformation are not dependent on other things and can be easily accomplished by each executor on their partition. For example `where clause transformation`
        * **Wide Dependency Transformation** These are transformations that requires data from other partitions to produce valid results. For example `groupby transformation` With Wide Dependency Transformation the data from different partitons need to be combined and *Repartitioned*(Also known as **Shuffle/Sort Exchange**) so that each output partition is having values from the same group. The actual transformation may then be applied to these column and can then be combined. Example-.count().
          * *We can control the number of partitions to be created as outcome of the shuffle sort using `spark.sql.shuffle.partitions` property.*

      * **Actions**: Spark Actions are what triggers the execution of the DAG of transformation. The code we write in our spark application may not be executed as is, the spark operations are internally reorganized and an optimized execution plan is  created when an Action is encountered and then these operations are executed by the executors.  Following are the actions in park
        * read
        * write
        * collect
        * show
* [Using Spark UI to investigate Jobs and Tasks](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20192582#overview)

* [**Spark Execution Plan**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20192586#overview) The spark execution plan may be found in the stage menu in the spark ui.
  * **Job** Each Action results in a job. **Please note .read() and infer-schema would be 2 jobs and then collect would be another job, so based on our code there would be 3 jobs.
  * **Stage** Each Wide Transformation would result in separate stage. For example-repartition would be a separate stage, the where clause would be a separate stage and the reshuffle before collect would be a separate stage.
    * Stages may run in parallel depending on the number of data frames.
    
    * **Need to figure out why 3 jobs are getting created instead of 1 job with 3 stages for our code**  [Medium Blog to understand Jobs, Stages & Tasks](https://towardsdatascience.com/unraveling-the-staged-execution-in-apache-spark-eff98c4cdac9)

* [**Spark RDD(Resilient Distributed Dataset) API**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20399103#overview) RDDs are language **native objects** with no row column structure or schema. It's similar to java/scala collections. 
  * RDDs are internally broken down to form a distributed collection. 
  * RDDS partions similar to dataframes may be executed on multiple executors but RDDs are fault-tolerant as they also store information about how they are created. This means if an executor processing an RDD partition goes down, a new executor may be spinned up and the same partion may be executed again.
  * With spark dataframes we use the sparksession to load the data however while using rdd we need to use the sparkcontext 
  * RDD are pretty lowlevel, this means they don't natively support common formats like parquet, csv, avro etc but RDD apis allows reading any kind of file but logic needs to be implemented by the developer nothing is available out of the box .textFile loads the data as lines of text, no schema or row column structure
  * RDDs only supports basic transformations like .map() .reduce() .collect() etc most rdds transformation accepts a lambda function. To implement transformations like groupby and we can write our custom transformation within our lambda function and pass them to map(), reduceByKey() etc

* [**SparkSQL**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20399111#overview) Instead of using Spark Dataframes and then applying the transformation onto them using the operation methods, we may use spark sql with the SparkSession to do the transformation as well. This is often suited for scenario where we can define the transformation using a sql like structure. However this may not be suitable for more complex transformation where we may need to use dataframe operations instead.
  * Please note to use spark sql we need to use a table or view. If table/view is not present we may use `.createOrReplaceTempView()` to create a view for a given name and dataframe.
  * Code Syntax for using Spark SQL:
  
        df_survey = utils.load_survey_df(spark, "data/survey.csv")
        df_survey.createOrReplaceTempView("survey_tbl")
        survey_count_df = spark.sql("select Country,count(*) from survey_tbl where age<40 group by Country")
        count_list=survey_count_df.collect()

* [**Internal workings of Spark SQL Engine and Catalyst Optimizer**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20399117#overview) SparkSQL, Spark DataFrame. Spark Dataset API(works with only jvm based languages like Spark and Scala) is build on top of the RDD API and `Spark SQL Engine`.
  * **Spark SQL Engine** Spark SQL Engine is a compiler that optimizes our code and also generates efficient java byte code. The Spark SQL Engine does this in 4 phases:
    1. **Analysis:** In this phase the Spark SQL Engine analyses our code and the column names, views and sql functions are resolved. An abstract syntax stream is generated.
    2. **Logical Optimization:** In this phase rule based optimization take splace and the `catalyst optimzer` will use cost based optimization to assign cost to each plan. Logical Optimization includes standard sql optimization techniques such as predicate pushdowns, projection pruning, boolean expresion simplification and constant folding
    3. **Physical Planning:** In this phase the spark sql engine picks the most effective logical plan and generates a physical plan.
       * **Physical Plan:** The physical plan is a set of RDD operation which determines how the plan is going to execute in the spark cluster.  
    4. **Code Generation** This step involves generating efficient java byte code to run on each machine within the cluster. This phase was introduced in spark 2.0 with project [Tungsten](https://www.databricks.com/glossary/tungsten)


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

* **Spark [DataFrameReader:](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20434113#overview)** This allows us to read data from external storage systems like file-systems, keyvalue stores, etc [Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html#pyspark.sql.DataFrameReader) 