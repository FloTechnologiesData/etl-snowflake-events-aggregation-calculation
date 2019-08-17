# FLOWWAY-FLOW-RATE-AGGREGATION
## INTRODUCTION 
This is the batch job related to Snowflake to aggregate the flow rate of water inside water pipes. The job does the following tasks:
1. Read Historical data from Snowflake and save to Snowflake
2. Read Continuous data from Snowflake and save to Snowflake
3. Hourly Job Schedule on Hour Basis

### SNOWFLAKE-HISTORICAL
This job will read data from given `sfSourceDbTable`. After Applying certain transformations to calculate start and end time for a group of data, it aggregates `FLOW_RATE`, `TEMPERATURE` and `PRESSURE` and saves data to `sfDestinationDbTable` table. 

### SNOWFLAKE-CONTINUOUS
This job will first read `max(END_TIMESTAMP)` and `min(START_TIMESTAMP)` from `sfDestinationDbTable` and then read data from that `max(END_TIMESTAMP)` and `min(START_TIMESTAMP)` from given `sfSourceDbTable`. After Applying certain transformations to calculate start and end time for a group of data, it aggregates `FLOW_RATE`, `TEMPERATURE` and `PRESSURE` and saves data to `sfDestinationDbTable` table. Before saving data, it deletes the data between `max(END_TIMESTAMP)` and `min(START_TIMESTAMP)` from `sfDestinationTable` 

### SNOWFLAKE-HOURLY
This Job First read the checkpoint from either `sfCheckpointDbTable` or `sfDestinationDbTable`. It then Read and process data after checkpoint Time from `sfSourceDbTable`. 

## JOB DEPENDENCIES
For jobs to run we need to have `Spark` installed by default and `SPARK_HOME` must be set as as environment variable to Spark installation path. We are using IntelliJ for development but any other code editor can also be used. Lastly, it extracts the min and max timestamps from processed data and Apply same operations as of in `SNOWFLAKE-CONTINUOUS` job.

* **Dependencies**
```$xslt
Java: >=1.8
Scala: 2.11.8
Sbt: 1.2.8
Spark SQL: 2.3.3
Snowflake Spark Connector: 2.4.14-spark_2.3
Scala-compiler: 2.11.8

```

* **Constants [ update it in org/data_flow/constants/Constants.scala ]**
```$xslt
SNOWFLAKE_URL
SNOWFLAKE_USERNAME
SNOWFLAKE_PASSWORD
```

* **Command Line Arguments**

These all arguments are not required for every Job to run, Below are sample spark-submit commands provided to run job with specific CLI arguments

```
--sfDatabase
--sfSchema
--sfWarehouse
--sfSourceDbTable
--sfDestinationDbTable
--sfCheckpointDbTable
--fromTimestamp
--toTimestamp
--endTimestamp
--endTimestamp1
--startTimestamp
--startTimestamp1
```

* **SBT dependency from maven for Snowflake Connector**
```$xslt
"net.snowflake" %% "spark-snowflake" % "2.4.14-spark_2.4"
```

* **Scala-compiler dependency from maven**

This library is used as a workaround for `scala.tools.nsc.Properties classdef not found` exception during snowflake Read/Write.

```$xslt
"org.scala-lang" % "scala-compiler" % "2.11.8"
```

* **Process to build jar and run on GCP cluster**

a) Go to `${PROJECT_HOME}` and build the project jar using `sbt assembly`

b) Jar will be place at `${PROJECT_HOME}/target/scala-2.11/${JAR_NAME}.jar`

c) now move the jar to GCP by using
`scp ${PROJECT_HOME}/target/scala-2.11/${JAR_NAME}.jar ${cluster-ip}:${/home/path/}`

d) You can check the `${/home/path/}` by hitting pwd command on cluster ssh console.

e) Now, ssh to `${cluster-ip}` and submit the spark job using one of the below discussed commands.

### SPARK-JOB-SUBMIT

* **Submitting Historical Job**

```$xslt
spark-submit --class org.data_flow.SnowflakeToSnowflakeHistorical ${JAR_NAME}.jar --sfDatabase= --sfSchema= --sfWarehouse= --sfSourceDbTable= --sfDestinationDbTable= --fromTimestamp="2019-06-01 00:00:00" --toTimestamp="2019-06-02 00:00:00"
```

* **Submitting Continuous Job**

```$xslt
spark-submit --class org.data_flow.SnowflakeContinuous ${JAR_NAME}.jar --sfDatabase= --sfSchema= --sfWarehouse= --sfSourceDbTable= --sfDestinationDbTable= --startTimestamp="2019-06-01 00:00:00" --startTimestamp1="2019-06-01 00:00:01" --endTimestamp="2019-05-31 23:59:59" --endTimestamp1="2019-06-01 00:00:00"

```

* **Submitting Hourly Job**

```
spark-submit --class org.data_flow.SnowflakeHourly ${JAR_NAME}.jar --sfDatabase= --sfSchema= --sfWarehouse= --sfSourceDbTable= --sfDestinationDbTable= --sfCheckpointDbTable
```