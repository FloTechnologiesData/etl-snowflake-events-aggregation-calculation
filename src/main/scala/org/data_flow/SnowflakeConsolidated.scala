package org.data_flow

import net.snowflake.spark.snowflake.SnowflakeConnectorUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.SparkSession
import org.data_flow.constants.{Constants, Queries}
import org.data_flow.services.dto.{DateStringToString, FloUtils, SnowflakeConsolidatedConfig, SnowflakeIncrementalConfig}
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
object SnowflakeConsolidated extends App with Constants with Queries {

  private val dataTransformation = new DataTransformation()
  private val snowflakeReader = new SparkSnowflakeConnector()
  private val snowflakeConnector = new SnowflakeConnector()
  private val dateStringToString = new DateStringToString()
  private val floUtils = new FloUtils()

  /**
    * Arguments
    */

  //  local[*]
  var consolidateTimeStamp: String = ""
  var sfDatabase: String = ""
  var sfSchema: String = ""
  var sfWarehouse: String = ""
  var sfSourceDbTable: String = ""
  var sfDestinationDbTable: String = ""
  var sfDelDbTable: String = ""

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
  //  select * from destination where (start>='2019-06-01 00:00:00' and Start<'2019-06-01 00:00:01') or (end>='2019-05-31 23:59:59' and end<'2019-06-01 00:00:00')

  val query = "select a.device_id, start_timestamp, end_timestamp from (" +
    "select distinct device_id, min(END_TIMESTAMP) as end_timestamp from " + sfDestinationDbTable +
    " where start_timestamp>='" + consolidateTimeStamp + "' group by device_id) a inner join " +
    "(select distinct device_id, max(start_timestamp) as start_timestamp from " + sfDestinationDbTable +
    " where end_timestamp<'" + consolidateTimeStamp + "' group by device_id) b " +
    "on a.device_id=b.device_id" +
    " where start_timestamp<'" + consolidateTimeStamp + "' order by a.device_id, start_timestamp"

  /**
    * Reading data from snowflake
    */
  var data = snowflakeReader.readData(query, spark, options)

  data = data.orderBy("DEVICE_ID", "START_TIMESTAMP")

  data.createOrReplaceTempView("dataToRemove")

  data.persist()

  val minMaxTimestamp = spark.sql(minMaxTimestampQuery).collectAsList().get(0)
  //
  var minStartTimestamp = minMaxTimestamp(0)
  var maxEndTimestamp = minMaxTimestamp(1)
  //
  val deviceIdsList = spark.sql("select distinct DEVICE_ID from dataToRemove").collectAsList()
  val deviceIds = floUtils.replaceChars(deviceIdsList.toString)
  //
  val d: Double = spark.sql("select (cast(cast('" + maxEndTimestamp + "' as timestamp) as long)-" +
    "cast(cast('" + minStartTimestamp + "' as timestamp) as long))/86400 as dayDiff").collectAsList().get(0).getDouble(0)
  //
  var dataToProcessAgainQuery = ""

  /**
    * HERE we are setting maximum limit to back-fill the Data
    * For Example:- TIMESTAMP>='2019-05-31 00:00:00' and TIMESTAMP<'2019-06-02 00:00:00' will be loaded
    * (i.e. max 2 days for any time job)
    */
  if (d <= 2.0) {
    dataToProcessAgainQuery = "select * from " + sfSourceDbTable + " where TIMESTAMP>='" +
      minStartTimestamp + "' and TIMESTAMP<='" +
      maxEndTimestamp + "' and DEVICE_ID in " +
      "('" + deviceIds + "')"
  } else {
    /**
      * Update min and start timestamps
      */
    minStartTimestamp = dateStringToString.dateStrToStr(minStartTimestamp.toString.slice(0, 19)).slice(0, 10) + "00:00:00"
    maxEndTimestamp = dateStringToString.addOneDay(minStartTimestamp.toString.slice(0, 19)) + "00:00:00"

    dataToProcessAgainQuery = "select * from " + sfSourceDbTable + " where TIMESTAMP>='" +
      minStartTimestamp + "' and TIMESTAMP<='" +
      maxEndTimestamp + "' and DEVICE_ID in " +
      "('" + deviceIds + "')"
  }

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

  snowflakeReader.writeData(sfDestinationDbTable, dataToWrite, options)

  stmt.execute("delete from DL.TELEMETRY.FLO_EVENTS_DEV FED using (select  count(*),device_id, start_timestamp,min(end_timestamp) as end_timestamp from DL.TELEMETRY.FLO_EVENTS_DEV group by device_id, start_timestamp having count(*)>1) fetd where FED.device_id=fetd.device_id and FED.start_timestamp=fetd.start_timestamp and FED.end_timestamp=fetd.end_timestamp")

  stmt.execute("delete from DL.TELEMETRY.FLO_EVENTS_DEV FED using (select  count(*),device_id, end_timestamp,max(start_timestamp) as start_timestamp from DL.TELEMETRY.FLO_EVENTS_DEV group by device_id, end_timestamp having count(*)>1) fetd where FED.device_id=fetd.device_id and FED.start_timestamp=fetd.start_timestamp and FED.end_timestamp=fetd.end_timestamp")

  data.unpersist()

  conn.close()

  /**
    * Reading Arguments
    *
    * @param conf - Config Arguments
    */
  def run(conf: SnowflakeConsolidatedConfig): Unit = {

    consolidateTimeStamp = conf.consolidateTimeStamp
    sfSchema = conf.sfSchema
    sfDatabase = conf.sfDatabase
    sfWarehouse = conf.sfWarehouse
    sfSourceDbTable = conf.sfSourceDbTable
    sfDestinationDbTable = conf.sfDestinationDbTable
    sfDelDbTable = conf.sfDelDbTable

  }

  /**
    * Setting Up the arguments
    */
  def setUpArguments(): Unit = {

    val parser = new scopt.OptionParser[SnowflakeConsolidatedConfig]("scopt") {
      head("scopt", "3.x")

      opt[String]('T', "consolidateTimeStamp") required() validate {
        x => {
          if (x == "") failure("consolidateTimeStamp can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(consolidateTimeStamp = x)
      } text "consolidateTimeStamp required"

      opt[String]('D', "sfDelDbTable") required() validate {
        x => {
          if (x == "") failure("sfDelDbTable can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(sfDelDbTable = x)
      } text "sfDelDbTable required"

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
      parser.parse(args, SnowflakeConsolidatedConfig()) match {
        case Some(config) =>
          run(config)
        case None =>
      }
    } catch {
      case e: Exception => println(e)
    }
  }

}

