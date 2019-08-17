package org.data_flow

import net.snowflake.spark.snowflake.SnowflakeConnectorUtils
import org.apache.spark.sql.SparkSession
import org.data_flow.constants.{Constants, Queries}
import org.data_flow.services.dto.{FloUtils, SnowflakeConfig}
import org.data_flow.transformations.HourlyTransformations

/**
  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  * JOB FLOW
  * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  * 1. Read checkpoint from snowflake
  * 2. If checkpoint is not defined then
  * read max(END_TIMESTAMP) from destination
  * table of snowflake
  * 3. Save that timestamp
  * 4. Read data after that checkpoint from
  * source table
  * 5. Transform and aggregate that data
  * 6. Now, get the min(START_TIMESTAMP) from
  * aggregated data and generate startTimestamp,
  * startTimestamp1, endTimestamp and endTimestamp1
  * 7. Read data between these four from destination table
  * 8. Get all DEVICE_ID's, min(START_TIMESTAMP) and
  * max(END_TIMESTAMP)
  * 9. Read data between these timestamps from source table
  * and aggregate that data
  * 10. Now, Discard duplicate type of records from data generated
  * in step 5 and step 9
  * 11. Delete all data from destination table corresponds
  * to data in step 8
  * 12. Finally, write data of step 10 to destination table
  *
  * Currently, STEP 10 is not being followed
  * ~~~~~~~~~~~
  * STATS
  * ~~~~~~~~~~~
  * 4/5 times read from snowflake
  * 2 times writes to snowflake
  *
  */
object SnowflakeHourly extends App with Constants with Queries {

  private val hourlyTransformations = new HourlyTransformations()
  private val floUtils = new FloUtils()

  /**
    * Arguments
    */
  var sfDatabase: String = ""
  var sfSchema: String = ""
  var sfWarehouse: String = ""
  var sfSourceDbTable: String = ""
  var sfDestinationDbTable: String = ""
  var sfCheckpointDbTable: String = ""

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
  val spark: SparkSession = floUtils.getSparkSession("data-flow-hourly-monitoring", "yarn")

  /**
    * Setting up snowflake connector utils
    */
  SnowflakeConnectorUtils.enablePushdownSession(spark)

  /**
    * Hourly Job Process
    */
  hourlyTransformations.processHourlyData(spark, options, sfSourceDbTable, sfDestinationDbTable, sfCheckpointDbTable,
    sfDatabase, sfSchema, sfWarehouse)

  /**
    * Reading Arguments
    * @param conf - Config Arguments
    */
  def run(conf: SnowflakeConfig): Unit = {

    sfSchema = conf.sfSchema
    sfDatabase = conf.sfDatabase
    sfWarehouse = conf.sfWarehouse
    sfSourceDbTable = conf.sfSourceDbTable
    sfDestinationDbTable = conf.sfDestinationDbTable
    sfCheckpointDbTable = conf.sfCheckpointDbTable

  }

  /**
    * Setting Up the arguments
    */
  def setUpArguments(): Unit = {

    val parser = new scopt.OptionParser[SnowflakeConfig]("scopt") {
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

      opt[String]('C', "sfCheckpointDbTable") required() validate {
        x => {
          if (x == "") failure("sfCheckpointDbTable can not be blank" + x)
          else success
        }
      } action { (x, c) =>
        c.copy(sfCheckpointDbTable = x)
      } text "sfCheckpointDbTable required"

    }

    try {
      parser.parse(args, SnowflakeConfig()) match {
        case Some(config) =>
          run(config)
        case None =>
      }
    } catch {
      case e: Exception => println(e)
    }
  }

}
