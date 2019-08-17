package org.data_flow.constants

import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

trait Constants {

  implicit val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  /**
    * CONSTANTS
    */
  val sfUrl: String = "gx82091.snowflakecomputing.com"
  val sfUserName: String = "nsingh"
  val sfPassword: String = ""

  /**
    * Source Table Schema
    */
  val schema = new StructType(
    Array(
      StructField("DEVICE_ID", StringType),
      StructField("TIMESTAMP", TimestampType),
      StructField("FLOW_RATE", DoubleType),
      StructField("PRESSURE", DoubleType),
      StructField("TEMPERATURE", IntegerType),
      StructField("MODE", IntegerType),
      StructField("VALVE_STATE", IntegerType)
    )
  )

  /**
    * Destination Table Schema
    */
  val destinationSchema = new StructType(
    Array(
      StructField("DEVICE_ID", StringType),
      StructField("START_TIMESTAMP", TimestampType),
      StructField("END_TIMESTAMP", TimestampType),
      StructField("RECORDS", IntegerType),
      StructField("MAX_FLOW_RATE", DoubleType),
      StructField("AVG_FLOW_RATE", DoubleType),
      StructField("SUM_FLOW_RATE", DoubleType),
      StructField("MIN_TEMPERATURE", DoubleType),
      StructField("MAX_TEMPERATURE", DoubleType),
      StructField("AVG_TEMPERATURE", DoubleType),
      StructField("MIN_PRESSURE", DoubleType),
      StructField("MAX_PRESSURE", DoubleType),
      StructField("AVG_PRESSURE", DoubleType),
      StructField("SD_PRESSURE", DoubleType),
      StructField("SD_FLOW_RATE", DoubleType),
      StructField("SYSTEM_MODE_2", IntegerType),
      StructField("SYSTEM_MODE_3", IntegerType),
      StructField("SYSTEM_MODE_5", IntegerType),
      StructField("DURATION", IntegerType)
    )
  )

  /**
    * Snowflake format
    */
  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

}
