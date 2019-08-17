package org.data_flow.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_flow.constants.Queries
import org.data_flow.services.snowflake.jdbc.SnowflakeConnector
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.data_flow.services.dto.{DateStringToString, FloUtils}
import org.data_flow.services.snowflake.spark.connector.SparkSnowflakeConnector

class HourlyTransformations extends SparkSnowflakeConnector with Queries {

  private val dataTransformation = new DataTransformation()
  private val snowflakeConnector = new SnowflakeConnector()
  private val dateStringToString = new DateStringToString()
  private val floUtils = new FloUtils()

  def processHourlyData(spark: SparkSession, options: Map[String, String], sfSourceDbTable: String,
                        sfDestinationDbTable: String, sfCheckpointDbTable: String, sfDatabase: String,
                        sfSchema: String, sfWarehouse: String): Unit = {

    import spark.implicits._
    /**
      * Reading last record timestamp from checkpoint in snowflake
      */
    var data: DataFrame = readData(lastRecordTimestampQueryCheckpoint + "" + sfCheckpointDbTable + " limit 1", spark, options)

    /**
      * check if checkpoint is retrieved from checkpoint table or not.
      * If not, get max(END_TIMESTAMP) from destination table
      */
    if (data.filter("timestamp != 'null'").count() == 0) {
      /**
        * Reading last record timestamp from destination table in snowflake
        */
      data = readData(lastRecordTimestampQueryDestination + "" + sfDestinationDbTable + " limit 1", spark, options)

    }

    data.createOrReplaceTempView("lastRecordTimestamp")

    /**
      * save timestamp to a variable for further processing
      */
    val collectLastRecordTimestamp = spark
      .sql("select timestamp from lastRecordTimestamp")
      .collectAsList()

    val lastRecordTimestamp = collectLastRecordTimestamp.get(0)(0)

    /**
      * Read data from snowflake after checkpoint timestamp
      */
    var currentData = readData(selectStarFromQuery + sfSourceDbTable + currentDataQuery + lastRecordTimestamp + "'",
      spark, options)

    /**
      * Apply Aggregations to the data
      */
    currentData = dataTransformation.processData(currentData, spark)

    writeData(sfDestinationDbTable, currentData, options)

    val getTimestamps = currentData.select(
      min("START_TIMESTAMP").alias("START_TIMESTAMP"),
      max("END_TIMESTAMP").alias("END_TIMESTAMP")
    ).collectAsList().get(0)

    /**
      * Get four date end points
      */
    val startTimestamp = getTimestamps(0).toString
    val startTimestamp1 = dateStringToString.addOneSecond(startTimestamp)
    val endTimestamp: String = dateStringToString.dateStrToStr(startTimestamp)
    val endTimestamp1 = startTimestamp

    /**
      * Get data from destination table with startTimestamp, startTimestamp1,
      * endTimestamp, endTimestamp1
      */
    val dataWithinRangeQuery = "select * from " + sfDestinationDbTable + " where (START_TIMESTAMP>='" +
      startTimestamp + "' and START_TIMESTAMP<'" + startTimestamp1 + "') or (" +
      "END_TIMESTAMP>='" + endTimestamp + "'and END_TIMESTAMP<'" + endTimestamp1 + "')"

    var rangeData = readData(dataWithinRangeQuery, spark, options)

    rangeData = rangeData.orderBy("DEVICE_ID", "START_TIMESTAMP")

    rangeData.createOrReplaceTempView("dataToUpdate")

    rangeData.persist()

    val rangeDataLevelOne = spark.sql(rangeDataLevelOneQuery)

    rangeDataLevelOne.createOrReplaceTempView("dataToRemove")

    /**
      * Get min and max dates to read data from source table
      */
    val minMaxTimestamp = spark.sql(minMaxTimestampQuery).collectAsList().get(0)

    val minStartTimestamp = minMaxTimestamp(0)
    val maxEndTimestamp = minMaxTimestamp(1)

    val deviceIdsList = spark.sql("select distinct DEVICE_ID from dataToRemove").collectAsList()

    val deviceIds = floUtils.replaceChars(deviceIdsList.toString)

    /**
      * Read data from source table
      */
    val dataToProcessAgainQuery = "select * from " + sfSourceDbTable + " where TIMESTAMP>='" +
      minStartTimestamp + "' and TIMESTAMP<='" +
      maxEndTimestamp + "' and DEVICE_ID in " +
      "('" + deviceIds + "')"

    var dataToProcessAgain = readData(dataToProcessAgainQuery, spark, options)

    /**
      * Transform data again
      */
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

    /**
      * Write Data To snowflake
      */
    writeData(sfDestinationDbTable, processedData, options)
    rangeData.unpersist()
    /**
      * Write Checkpoint
      */
    val checkpoint = Seq(
      getTimestamps(1).toString
    ).toDF("TIMESTAMP")

    writeData(sfCheckpointDbTable, checkpoint, options)

  }

}
