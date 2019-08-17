package org.data_flow.services.snowflake.spark.connector

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_flow.constants.Constants

class SparkSnowflakeConnector extends Constants {

  /**
    * Read the data from snowflake
    * @param query - Query for snowflake
    * @param spark - [[SparkSession]]
    * @return - snowflake dataset as [[DataFrame]]
    */
  def readData(query: String, spark: SparkSession, options: Map[String, String]): DataFrame = {

    spark
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .option("insecureMode", value = true)
      .options(options)
      .option("query", query)
      .load()

  }

  /**
    * Write data to snowflake Table
    * @param tableName - Snowflake Table name to write data on
    * @param data - [[DataFrame]] to write
    */
  def writeData(tableName: String, data: DataFrame, options: Map[String, String]): Unit = {

    data
      .write
      .mode("append")
      .format(SNOWFLAKE_SOURCE_NAME)
      .option("insecureMode", value = true)
      .option("column_mapping", "name") // this will automatically map column values to respective field
      .options(options)
      .option("dbtable", tableName)
      .save()

  }

}
