# Prototype for saving Graphite data from Kafka into Parquet

See [blog post](http://mabrek.github.io/blog/kafka-parquet-timeseries/) for motivation and more details.

## Dependencies

*  Java 8
*  [SBT](http://www.scala-sbt.org/download.html) to build project
*  [Kafka](http://kafka.apache.org/) on a single node as described in [quickstart guide](http://kafka.apache.org/documentation.html#quickstart)
*  [netcat](http://netcat.sourceforge.net/)
*  [kafkacat](https://github.com/edenhill/kafkacat)
*  anything that produces data in [Graphite plaintext protocol](http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol) like [collectd](https://collectd.org/) with [write graphite plugin](https://collectd.org/wiki/index.php/Plugin:Write_Graphite)

## Running

Getting Graphite data into Kafka:

    nc -4l localhost 2003 | kafkacat -P -b localhost -t metrics -K ' '

It feeds all data received on localhost:2003 into topic `metrics` with metric name as message key and `value timestamp` as a payload.

Saving data from Kafka into Parquet files

    sbt -mem <jvmMemorySize> "run <topic> <partition> <offset> <fetchSize> <targetFolder>"

Where:

*  topic - topic name which contains messages with `metric_name` as key and `value timestamp` payload from graphite plaintext protocol
*  partition - partition number
*  offset - number of messages to skip from the beginning
*  fetchSize - (bytes) maximum size of data to fetch from kafka
*  targetFolder - path where to save Parquet files
*  jvmMemorySize - maximum memory for JVM (-Xmx argument), must be at least 3 times larger than fetchSize.

Parquet files will have name `$topic-$partition-$offset-$nextOffset.parquet` under `targetFolder`

`nextOffset` from file name is intended to be used as `offset` for subsequent invocations to save next batch of data.

Example:

    sbt -mem 2048 "run metrics 0 0 500000000 /tmp"
