import java.nio.charset.StandardCharsets

import kafka.api.FetchRequestBuilder
import kafka.consumer.SimpleConsumer
import kafka.network.BlockingChannel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties.WriterVersion._
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.ParquetWriter._
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.{MessageType, Type, Types}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.NonFatal

object KafkaTimeseries {
  def main(args: Array[String]) {
    try {
      if (args.size != 5) {
        System.err.println("required parameters: topic partition offset fetchSize targetFolder")
        System.exit(-1)
      }
      val topic = args(0)
      val partition = args(1).toInt
      val offset = args(2).toLong
      val fetchSize = args(3).toInt
      val targetFolder = args(4)
      
      val name = s"client-$topic-$partition"
      val consumer = new SimpleConsumer("localhost", 9092, 5000, BlockingChannel.UseDefaultBufferSize, name)
      val timestamps = mutable.SortedSet[Long]()
      val types = mutable.Set[Type]()
      val columns = mutable.Map[String, mutable.Map[Long, Double]]()
      val nextOffset = try {
        val fetchRequest = new FetchRequestBuilder().clientId(name)
          .addFetch(topic, partition, offset, fetchSize).build()
        val fetchResponse = consumer.fetch(fetchRequest)
        if (fetchResponse.hasError) {
          throw new Exception("fetch request error code" + fetchResponse.errorCode(topic, partition))
        }
        fetchResponse.messageSet(topic, partition)
          .foldLeft(offset) {
            case (maxOffset, messageAndOffset) =>
              val keyBytes = new Array[Byte](messageAndOffset.message.keySize)
              messageAndOffset.message.key.get(keyBytes)
              val key = new String(keyBytes, StandardCharsets.US_ASCII)
              val payloadBytes = new Array[Byte](messageAndOffset.message.payloadSize)
              messageAndOffset.message.payload.get(payloadBytes)
              val payload = new String(payloadBytes, StandardCharsets.US_ASCII)
              try {
                val split = payload.trim.split("\\s+")
                val value = split(0).toDouble
                val timestamp = split(1).toLong
                timestamps += timestamp
                types += Types.optional(DOUBLE).named(key)
                columns.getOrElseUpdate(key, mutable.Map[Long, Double]()).update(timestamp, value)
              } catch {
                case e: NumberFormatException =>
                  System.err.println(s"number format error $e in key: $key payload: $payload")
              }
              Math.max(maxOffset, messageAndOffset.nextOffset)
        }
      } finally {
        consumer.close()
      }
      
      val configuration = new Configuration
      val schema = new MessageType("GraphiteLine", (types + Types.required(INT64).named("timestamp")).toList) 
      GroupWriteSupport.setSchema(schema, configuration)
      val gf = new SimpleGroupFactory(schema)
      val outFile = new Path(targetFolder, s"$topic-$partition-$offset-$nextOffset.parquet")
      val writer = new ParquetWriter[Group](outFile, new GroupWriteSupport, GZIP, DEFAULT_BLOCK_SIZE,
        DEFAULT_PAGE_SIZE, 512, true, false, PARQUET_2_0, configuration)
      try {
        for (timestamp <- timestamps) {
          val group = gf.newGroup().append("timestamp", timestamp)
          for ((metric, column) <- columns) {
            column.get(timestamp).foreach(group.append(metric, _))
          }
          writer.write(group)
        }
      } finally {
        writer.close()
      }
      
    } catch {
      case NonFatal(e) => 
        System.err.println("oops: " + e)
        e.printStackTrace()
    }
  }
}
