name := "kafka-timeseries"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.2"

libraryDependencies += "org.apache.parquet" % "parquet-column" % "1.7.0"

libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.7.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.3.0"
