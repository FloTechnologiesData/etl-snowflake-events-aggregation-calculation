name := "data-flow-monitoring"

version := "0.3.8"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3" % Provided
libraryDependencies += "net.snowflake" %% "spark-snowflake" % "2.4.14-spark_2.3"
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.8" % Provided
libraryDependencies += "net.snowflake" % "snowflake-jdbc" % "3.8.4" % Provided
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"
//libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.17"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}