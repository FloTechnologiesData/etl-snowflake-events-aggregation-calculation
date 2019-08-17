package org.data_flow.services.dto

/**
  * Case class for Hourly Data Arguments
  *
  * @param sfDatabase           - Snowflake Database Name
  * @param sfSchema             - Snowflake Schema Name
  * @param sfWarehouse          - Snowflake Warehouse Name
  * @param sfSourceDbTable      - Snowflake Source Table Name
  * @param sfDestinationDbTable - Snowflake Destination Table Name
  * @param sfCheckpointDbTable  - Snowflake Checkpoint Table Name
  */
case class SnowflakeConfig(
                            sfDatabase: String = "",
                            sfSchema: String = "",
                            sfWarehouse: String = "",
                            sfSourceDbTable: String = "",
                            sfDestinationDbTable: String = "",
                            sfCheckpointDbTable: String = ""
                          )

/**
  * Case class for Historical Job Arguments
  *
  * @param sfDatabase           - Snowflake Database Name
  * @param sfSchema             - Snowflake Schema Name
  * @param sfWarehouse          - Snowflake Warehouse Name
  * @param sfSourceDbTable      - Snowflake Source Table Name
  * @param sfDestinationDbTable - Snowflake Destination Table Name
  * @param fromTimestamp        - Timestamp from data needs to be processed
  * @param toTimestamp          - Timestamp to data needs to be processed
  */
case class SnowflakeConfigHistorical(
                                      sfDatabase: String = "",
                                      sfSchema: String = "",
                                      sfWarehouse: String = "",
                                      sfSourceDbTable: String = "",
                                      sfDestinationDbTable: String = "",
                                      fromTimestamp: String = "",
                                      toTimestamp: String = ""
                                    )

/**
  * Case Class for Incremental Job Arguments
  *
  * @param endTimestamp         - to Timestamp
  * @param endTimestamp1        - to Timestamp  + 1 Second
  * @param startTimestamp       - from Timestamp
  * @param startTimestamp1      - from Timestamp + 1 Second
  * @param sfDatabase           - Snowflake Database Name
  * @param sfSchema             - Snowflake Schema Name
  * @param sfWarehouse          - Snowflake Warehouse Name
  * @param sfSourceDbTable      - Snowflake Source Table Name
  * @param sfDestinationDbTable - Snowflake Destination Table Name
  */
case class SnowflakeIncrementalConfig(
                                       endTimestamp: String = "",
                                       endTimestamp1: String = "",
                                       startTimestamp: String = "",
                                       startTimestamp1: String = "",
                                       sfDatabase: String = "",
                                       sfSchema: String = "",
                                       sfWarehouse: String = "",
                                       sfSourceDbTable: String = "",
                                       sfDestinationDbTable: String = ""
                                     )

case class SnowflakeConsolidatedConfig(
                                        consolidateTimeStamp: String = "",
                                        sfDatabase: String = "",
                                        sfSchema: String = "",
                                        sfWarehouse: String = "",
                                        sfSourceDbTable: String = "",
                                        sfDestinationDbTable: String = "",
                                        sfDelDbTable: String = ""
                                      )