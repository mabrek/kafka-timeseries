import java.nio.charset.StandardCharsets

import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.network.BlockingChannel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties.WriterVersion._
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.ParquetWriter._
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.schema.{Types, MessageType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.hadoop.metadata.CompressionCodecName._

import scala.util.control.NonFatal

object KafkaTimeseries {
  def main(args: Array[String]) {
    val topic = args(0)
    val partition = Integer.parseInt(args(1))
    val offset = args(2).toLong
    val topicAndPartition = new TopicAndPartition(topic, partition)
    val name = "client-" + topic + "-" + partition
    val consumer = new SimpleConsumer("localhost", 9092, 5000, BlockingChannel.UseDefaultBufferSize, name)
    val configuration = new Configuration
    val schema = Types.buildMessage()
        .required(BINARY).as(UTF8).named("metric")
        .required(DOUBLE).named("value")
        .required(INT64).named("timestamp")
      .named("GraphiteLine")
    GroupWriteSupport.setSchema(schema, configuration)
    val gf = new SimpleGroupFactory(schema)
    val outFile = new Path("/tmp/graphite-parquet")
    val writer = new ParquetWriter[Group](outFile, new GroupWriteSupport, UNCOMPRESSED, DEFAULT_BLOCK_SIZE, 
      DEFAULT_PAGE_SIZE, 512, true, false, PARQUET_2_0, configuration)
    try {
      val fetchRequest = new FetchRequestBuilder().clientId(name).addFetch(topic, partition, offset, 8096).build()
      val fetchResponse = consumer.fetch(fetchRequest)
      if (fetchResponse.hasError) {
        throw new Exception("fetch request error code" + fetchResponse.errorCode(topic, partition))
      }
      val nextOffset = fetchResponse.messageSet(topic, partition).foldLeft(offset) {
        (o:Long, mo:MessageAndOffset) =>
          val keyBytes = new Array[Byte](mo.message.keySize)
          mo.message.key.get(keyBytes)
          val key = new String(keyBytes, StandardCharsets.US_ASCII)
          val payloadBytes = new Array[Byte](mo.message.payloadSize)
          mo.message.payload.get(payloadBytes)
          val payload = new String(payloadBytes, StandardCharsets.US_ASCII)
          try {
            val split = payload.trim.split("\\s+")
            val value = split(0).toDouble
            val timestamp = split(1).toLong
            writer.write(gf.newGroup().append("metric", key).append("value", value).append("timestamp", timestamp))
          } catch {
            case e:NumberFormatException =>
              System.err.println(s"number format error $e in key: $key payload: $payload")
          }
          Math.max(o, mo.nextOffset)
      }
      System.out.println("next offset " + nextOffset)
    } catch {
      case NonFatal(e) => 
        System.err.println("oops: " + e)
        e.printStackTrace()
    } finally {
      consumer.close()
      writer.close()
    }
  }
}
