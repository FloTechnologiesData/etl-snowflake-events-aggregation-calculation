package org.data_flow

import net.snowflake.spark.snowflake.SnowflakeConnectorUtils
import org.apache.spark.sql.SparkSession
import org.data_flow.constants.{Constants, Queries}
import org.data_flow.services.dto.{FloUtils, SnowflakeConfigHistorical}
import org.data_flow.services.snowflake.spark.connector.SparkSnowflakeConnector
import org.data_flow.transformations.DataTransformation

/**
  * This will read data from snowflake
  * and after applying certain transformations,
  * it will save that data into snowflake table
  *
  */
object SnowflakeToSnowflakeHistorical extends App with Constants with Queries {

  private val dataTransformation = new DataTransformation()
  private val snowflakeReader = new SparkSnowflakeConnector()
  private val floUtils = new FloUtils()

  /**
    * Arguments
    */


  //  --sfDatabase=DL --sfSchema=TELEMETRY --sfWarehouse=DATA_LOADING
  //  --sfSourceDbTable=FLO_DEVICES --sfDestinationDbTable=FLO_EVENTS_DEV --fromTimestamp="2019-06-01 00:00:00"
  //  --toTimestamp="2019-06-02 00:00:00"

  var sfDatabase: String = ""
  var sfSchema: String = ""
  var sfWarehouse: String = ""
  var sfSourceDbTable: String = ""
  var sfDestinationDbTable: String = ""
  var fromTimestamp: String = ""
  var toTimestamp: String = ""

  //  2019-05-31 00:00:00
  //var fromTimestamp: String = "2019-05-31 00:00:00"
  //var toTimestamp: String = "2019-06-01 00:00:00"
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
  val spark: SparkSession = floUtils.getSparkSession("data-flow-historical-load", "yarn")

  /**
    * Setting up snowflake connector utils
    */
  SnowflakeConnectorUtils.enablePushdownSession(spark)

  val dbQuery = selectStarFromQuery + "" + sfSourceDbTable + " where TIMESTAMP >='" +
    fromTimestamp + "' and TIMESTAMP <'" + toTimestamp + "'"

  val data = snowflakeReader.readData(dbQuery, spark, options)

  val processedData = dataTransformation.processData(data, spark)

  snowflakeReader.writeData(sfDestinationDbTable, processedData, options)

  /**
    * Reading Arguments
    *
    * @param conf - Config Arguments
    */
  def run(conf: SnowflakeConfigHistorical): Unit = {

    sfSchema = conf.sfSchema
    sfDatabase = conf.sfDatabase
    sfWarehouse = conf.sfWarehouse
    sfSourceDbTable = conf.sfSourceDbTable
    sfDestinationDbTable = conf.sfDestinationDbTable
    fromTimestamp = conf.fromTimestamp
    toTimestamp = conf.toTimestamp

  }

  /**
    * Setting Up the arguments
    */
  def setUpArguments(): Unit = {

    val parser = new scopt.OptionParser[SnowflakeConfigHistorical]("scopt") {
      head("scopt", "3.x")

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

      opt[String]('f', "fromTimestamp") required() validate {
        x => {
          if (x == "") failure("fromTimestamp can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(fromTimestamp = x)
      } text "fromTimestamp required"

      opt[String]('t', "toTimestamp") required() validate {
        x => {
          if (x == "") failure("toTimestamp can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(toTimestamp = x)
      } text "toTimestamp required"

    }

    try {
      parser.parse(args, SnowflakeConfigHistorical()) match {
        case Some(config) =>
          run(config)
        case None =>
      }
    } catch {
      case e: Exception => println(e)
    }
  }

}

