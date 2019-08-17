package org.data_flow.services.snowflake.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.data_flow.constants.Constants

class SnowflakeConnector extends Constants {

  private val driverName: String = "net.snowflake.client.jdbc.SnowflakeDriver"

  /**
    * Generate Snowflake jdbc connection
    * @param sfDatabase - Snowflake Database name
    * @param sfSchema - Snowflake Schema Name
    * @param sfWarehouse - Snowflake Warehouse Name
    * @return - [[Connection]]
    */
  def snowflakeConnector(sfDatabase: String, sfSchema: String, sfWarehouse: String): Connection = {

    val props = new Properties()
    props.put("user", sfUserName)
    props.put("password", sfPassword)
    props.put("db", sfDatabase)
    props.put("schema", sfSchema)
    props.put("warehouse", sfWarehouse)
    props.put("insecureMode", "true")

    val con = DriverManager.getConnection(
      "jdbc:snowflake://" + sfUrl, props
    )

    con

  }

}
