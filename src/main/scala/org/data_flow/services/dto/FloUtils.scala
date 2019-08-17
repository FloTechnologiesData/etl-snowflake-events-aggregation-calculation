package org.data_flow.services.dto

import org.apache.spark.sql.SparkSession
import org.data_flow.constants.Constants

/**
  * Utility class that merges extra files after write as csv
  */
class FloUtils extends Constants {

  /**
    * Added quotes in a list string
    * @param listStr - Input List type String
    * @return - String
    *
    *         Example: input: 'a, b, c'
    *                  output: 'a','b','c'
    */
  def replaceChars(listStr: String): String = {

    listStr
      .replace("[", "")
      .replace("]", "")
      .replace(" ", "")
      .replace(",", "','")

  }

  /**
    * returns map type object with Snowflake Configuration Values
    * @param sfDatabase - Snowflake Database name
    * @param sfSchema - Snowflake Schema Name
    * @param sfWarehouse - Snowflake Warehouse Name
    * @return - Map of String->String type Object
    */
  def getOptions(sfDatabase: String, sfSchema: String, sfWarehouse: String): Map[String, String] = {
    Map(
      "sfUrl" -> sfUrl,
      "sfUser" -> sfUserName,
      "sfPassword" -> sfPassword,
      "sfDatabase" -> sfDatabase,
      "sfSchema" -> sfSchema,
      "sfWarehouse" -> sfWarehouse
    )
  }

  /**
    * Returns SparkSession
    * @param appName - set app name
    * @param master - master value
    * @return - SparkSession
    */
  def getSparkSession(appName: String, master: String): SparkSession = {

    SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()

  }

}
