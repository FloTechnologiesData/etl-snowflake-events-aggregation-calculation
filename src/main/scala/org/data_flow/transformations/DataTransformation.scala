package org.data_flow.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, last, lead, when, typedLit}
import org.apache.spark.sql.types.LongType

class DataTransformation {

  /**
    * This will remove the unused data
    *
    * @param unprocessedData - [[DataFrame]]
    * @return - [[DataFrame]]
    */
  def removeUnusedData(unprocessedData: DataFrame): DataFrame = {

    val data: DataFrame = unprocessedData
    data
      .drop("VALVE_STATE")
      .orderBy("DEVICE_ID", "TIMESTAMP")

  }

  /**
    * This method will aggregate the data after
    * applying specific Transformations
    *
    * @param dataToAggregate - [[DataFrame]]
    * @param spark           - [[SparkSession]]
    * @return - [[DataFrame]]
    */
  def aggregateData(dataToAggregate: DataFrame, spark: SparkSession): DataFrame = {
    val data: DataFrame = dataToAggregate

    data.createOrReplaceTempView("snowflakeData")

    val dataToShow = spark.sql("select DEVICE_ID, start_time_stamp as START_TIMESTAMP, " +
      "end_time_stamp as END_TIMESTAMP, " +
      "count(*) as RECORDS," +
      " max(flow_rate) as MAX_FLOW_RATE, " +
      "avg(flow_rate) as AVG_FLOW_RATE, sum(flow_rate) as SUM_FLOW_RATE, " +
      "min(temperature) as MIN_TEMPERATURE, max(temperature) as MAX_TEMPERATURE, " +
      "avg(temperature) as AVG_TEMPERATURE," +
      "min(pressure) as MIN_PRESSURE, max(pressure) as MAX_PRESSURE, " +
      "avg(pressure) as AVG_PRESSURE, STD(pressure) as SD_PRESSURE, " +
      "STD(flow_rate) as SD_FLOW_RATE, count(case when mode = 2 then 1 end) as SYSTEM_MODE_2, " +
      "count(case when mode = 3 then 1 end) as SYSTEM_MODE_3, count(case when mode = 5 then 1 end) as SYSTEM_MODE_5 " +
      "from snowflakeData group by DEVICE_ID, " +
      "start_time_stamp, end_time_stamp order by DEVICE_ID")

    dataToShow
      .withColumn("DURATION",
        col("END_TIMESTAMP").cast(LongType) - col("START_TIMESTAMP").cast(LongType)
      )
      .withColumn("SD_PRESSURE",
        when(
          col("SD_PRESSURE").isNaN, 0.0).otherwise(
          col("SD_PRESSURE")
        )
      )
      .withColumn("SD_FLOW_RATE",
        when(
          col("SD_FLOW_RATE").isNaN, 0.0).otherwise(
          col("SD_FLOW_RATE")
        )
      )

  }

  /**
    * Complete T of ETL for current [[DataFrame]]
    *
    * @param processableData - [[DataFrame]]
    * @param spark           - [[SparkSession]]
    * @return - [[DataFrame]]
    */
  def processData(processableData: DataFrame, spark: SparkSession): DataFrame = {

    var data: DataFrame = processableData

    data = removeUnusedData(data)

    val window = Window.partitionBy("DEVICE_ID")
      .orderBy("DEVICE_ID", "TIMESTAMP")

    val windowTwo = Window.partitionBy("DEVICE_ID")
      .orderBy("DEVICE_ID", "start_time_stamp")

    data = data
      .withColumn("prev_flow_rate", lag(col("FLOW_RATE"), 1).over(window))
      .withColumn("next_flow_rate", lead(col("FLOW_RATE"), 1).over(window))
      .withColumn("is_skipped",
        when(col("FLOW_RATE").isin(0.0), typedLit(1)).otherwise(typedLit(0))
      )
      .filter("is_skipped=0")
      .withColumn("prev_time_stamp", lag("TIMESTAMP", 1).over(window))
      .withColumn("next_time_stamp", lead("TIMESTAMP", 1).over(window))
      .withColumn("start_time_stamp",
        when(
          col("prev_time_stamp").isNull or col("prev_flow_rate").isin(0.0),
          col("TIMESTAMP")
        ).otherwise(typedLit(null))
      )
      .withColumn("end_time_stamp",
        when(
          col("next_time_stamp").isNull or col("next_flow_rate").isin(0.0),
          col("TIMESTAMP")
        ).otherwise(typedLit(null))
      )
      .withColumn("start_time_stamp",
        when(col("prev_time_stamp").isNull, col("start_time_stamp"))
          .otherwise(last(col("start_time_stamp"), ignoreNulls = true).over(window))
      )
      .withColumn("end_time_stamp",
        when(col("prev_time_stamp").isNull, col("end_time_stamp"))
          .otherwise(last(col("end_time_stamp"), ignoreNulls = true).over(windowTwo))
      )
      .withColumn("end_time_stamp",
        when(
          col("end_time_stamp").isNull and col("prev_time_stamp").isNull,
          lead(col("end_time_stamp"), 1).over(window)
        ).otherwise(col("end_time_stamp"))
      )
      .drop("is_skipped", "prev_flow_rate", "next_flow_rate", "prev_time_stamp", "next_time_stamp")

    aggregateData(data, spark)

  }

}
