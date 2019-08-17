package org.data_flow.constants

trait Queries extends Constants {

  val lastRecordTimestampQueryCheckpoint: String =
    "select max(TIMESTAMP) as timestamp from "

  val lastRecordTimestampQueryDestination: String =
    "select max(END_TIMESTAMP) as timestamp from "

  val selectStarFromQuery: String =
    "select * from "

  val currentDataQuery: String =
    " where TIMESTAMP>'"

  val rangeDataLevelOneQuery: String =
    "select DEVICE_ID, min(START_TIMESTAMP) as START_TIMESTAMP, " +
      "max(END_TIMESTAMP) as END_TIMESTAMP from dataToUpdate group by DEVICE_ID"

  val minMaxTimestampQuery: String =
    "select min(START_TIMESTAMP), max(END_TIMESTAMP) from dataToRemove"

  val dataToWriteQuery: String =
    "select a.*, b.START_TIMESTAMP as ACT_START_TIMESTAMP," +
      "b.END_TIMESTAMP as ACT_END_TIMESTAMP from newProcessedData a inner join (" +
      "select * from dataToRemove" +
      ") b on a.DEVICE_ID=b.DEVICE_ID order by a.DEVICE_ID, a.START_TIMESTAMP"

}
