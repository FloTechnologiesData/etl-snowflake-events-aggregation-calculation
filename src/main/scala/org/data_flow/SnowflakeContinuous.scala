package org.data_flow

import net.snowflake.spark.snowflake.SnowflakeConnectorUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.SparkSession
import org.data_flow.constants.{Constants, Queries}
import org.data_flow.services.dto.{DateStringToString, FloUtils, SnowflakeIncrementalConfig}
import org.data_flow.services.snowflake.jdbc.SnowflakeConnector
import org.data_flow.services.snowflake.spark.connector.SparkSnowflakeConnector
import org.data_flow.transformations.DataTransformation

/**
  * This will read data from snowflake
  * and after applying certain transformations,
  * it will save that data into snowflake table
  *
  * The process is as follows:
  * * First We will get the max(end_timestamp) from snowflake
  * table where we are writing the final data
  * * Then, we read data from that timestamp from
  * source snowflake table
  * * When we have data we will first ignore the data
  * containing flow_rate as 0
  * * We, then apply transformations to get data in destination
  * table format and finally write to destination table
  *
  */
object SnowflakeContinuous extends App with Constants with Queries {

  private val dataTransformation = new DataTransformation()
  private val snowflakeReader = new SparkSnowflakeConnector()
  private val snowflakeConnector = new SnowflakeConnector()
  private val dateStringToString = new DateStringToString()
  private val floUtils = new FloUtils()

  /**
    * Arguments
    */
  var startTimestamp = ""
  var startTimestamp1 = ""
  var endTimestamp = ""
  var endTimestamp1 = ""
  var sfDatabase: String = ""
  var sfSchema: String = ""
  var sfWarehouse: String = ""
  var sfSourceDbTable: String = ""
  var sfDestinationDbTable: String = ""

  /**
    * Setting Up arguments
    */
  setUpArguments()

  /**
    * Snowflake Options
    */
  val options: Map[String, String] = floUtils.getOptions(sfDatabase, sfSchema, sfWarehouse)

  /**
    * Setting Up spark Session
    */
  val spark: SparkSession = floUtils.getSparkSession("data-flow-continuous-fix-process", "yarn")

  /**
    * Setting up snowflake connector utils
    */
  SnowflakeConnectorUtils.enablePushdownSession(spark)

  /**
    * Queries to read data from SNOWFLAKE source and destination tables
    * These queries must be write in such a way that they do less computation
    * provides only required data.
    *
    */
  val query = selectStarFromQuery + sfDestinationDbTable + " where (START_TIMESTAMP>='" + startTimestamp + "' and " +
    "START_TIMESTAMP<'" + startTimestamp1 + "') or (" +
    "END_TIMESTAMP>='" + endTimestamp + "'and END_TIMESTAMP<'" + endTimestamp1 + "')"

  /**
    * Reading data from snowflake
    */
  var data = snowflakeReader.readData(query, spark, options)

  data = data.orderBy("DEVICE_ID", "START_TIMESTAMP")

  data.createOrReplaceTempView("dataToUpdate")

  data.persist()

  var dataLevelOne = spark.sql(rangeDataLevelOneQuery)

  dataLevelOne.createOrReplaceTempView("dataToRemove")

  val minMaxTimestamp = spark.sql(minMaxTimestampQuery).collectAsList().get(0)

  val minStartTimestamp = minMaxTimestamp(0)
  val maxEndTimestamp = minMaxTimestamp(1)

  val deviceIdsList = spark.sql("select distinct DEVICE_ID from dataToRemove").collectAsList()
  val deviceIds = floUtils.replaceChars(deviceIdsList.toString)

  val dataToProcessAgainQuery = "select * from " + sfSourceDbTable + " where TIMESTAMP>='" +
    minStartTimestamp + "' and TIMESTAMP<='" +
    maxEndTimestamp + "' and DEVICE_ID in " +
    "('" + deviceIds + "')"

  val dataToProcessAgain = snowflakeReader.readData(dataToProcessAgainQuery, spark, options)
  val processedData = dataTransformation.processData(dataToProcessAgain, spark)
  processedData.createOrReplaceTempView("newProcessedData")

  var dataToWrite = spark.sql(dataToWriteQuery)
  dataToWrite = dataToWrite
    .withColumn("start_gap",
      col("ACT_START_TIMESTAMP").cast(LongType) - col("START_TIMESTAMP").cast(LongType))
    .withColumn("end_gap",
      col("ACT_END_TIMESTAMP").cast(LongType) - col("END_TIMESTAMP").cast(LongType))
    .filter("start_gap=0 and end_gap=0")
    .drop("ACT_START_TIMESTAMP", "ACT_END_TIMESTAMP", "start_gap", "end_gap")

  val conn = snowflakeConnector.snowflakeConnector(sfDatabase, sfSchema, sfWarehouse)

  val stmt = conn.createStatement()

  /**
    * We will now batch delete only device specific DataRange data
    */
  var dataToRemove = spark.sql("select * from dataToRemove").collect()

  dataToRemove.foreach(
    row =>
      stmt.addBatch("delete from " + sfDestinationDbTable + " where DEVICE_ID='" + row.getString(0) + "' " +
        "and START_TIMESTAMP>='" + row.getAs[TimestampType](1) + "' and END_TIMESTAMP<'"
        + dateStringToString.addOneSecond(row.getAs[TimestampType](2).toString.slice(0, 19)) + "'")
  )

  stmt.executeBatch()

  conn.close()

  snowflakeReader.writeData(sfDestinationDbTable, dataToWrite, options)

  data.unpersist()

  /**
    * Reading Arguments
    *
    * @param conf - Config Arguments
    */
  def run(conf: SnowflakeIncrementalConfig): Unit = {

    endTimestamp = conf.endTimestamp
    endTimestamp1 = conf.endTimestamp1
    startTimestamp = conf.startTimestamp
    startTimestamp1 = conf.startTimestamp1
    sfSchema = conf.sfSchema
    sfDatabase = conf.sfDatabase
    sfWarehouse = conf.sfWarehouse
    sfSourceDbTable = conf.sfSourceDbTable
    sfDestinationDbTable = conf.sfDestinationDbTable

  }

  /**
    * Setting Up the arguments
    */
  def setUpArguments(): Unit = {

    val parser = new scopt.OptionParser[SnowflakeIncrementalConfig]("scopt") {
      head("scopt", "3.x")

      opt[String]('e', "endTimestamp") required() validate {
        x => {
          if (x == "") failure("endTimestamp can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(endTimestamp = x)
      } text "endTimestamp required"

      opt[String]('E', "endTimestamp1") required() validate {
        x => {
          if (x == "") failure("endTimestamp1 can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(endTimestamp1 = x)
      } text "endTimestamp1 required"

      opt[String]('t', "startTimestamp") required() validate {
        x => {
          if (x == "") failure("startTimestamp can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(startTimestamp = x)
      } text "startTimestamp required"

      opt[String]('T', "startTimestamp1") required() validate {
        x => {
          if (x == "") failure("startTimestamp1 can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(startTimestamp1 = x)
      } text "startTimestamp1 required"

      opt[String]('d', "sfDatabase") required() validate {
        x => {
          if (x == "") failure("sfDatabase can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(sfDatabase = x)
      } text "sfDatabase required"

      opt[String]('s', "sfSchema") required() validate {
        x => {
          if (x == "") failure("sfSchema can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(sfSchema = x)
      } text "sfSchema required"

      opt[String]('w', "sfWarehouse") required() validate {
        x => {
          if (x == "") failure("sfWarehouse can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(sfWarehouse = x)
      } text "sfWarehouse required"

      opt[String]('S', "sfSourceDbTable") required() validate {
        x => {
          if (x == "") failure("sfSourceDbTable can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(sfSourceDbTable = x)
      } text "sfSourceDbTable required"

      opt[String]('D', "sfDestinationDbTable") required() validate {
        x => {
          if (x == "") failure("sfDestinationDbTable can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(sfDestinationDbTable = x)
      } text "sfDestinationDbTable required"

    }

    try {
      parser.parse(args, SnowflakeIncrementalConfig()) match {
        case Some(config) =>
          run(config)
        case None =>
      }
    } catch {
      case e: Exception => println(e)
    }
  }

}

