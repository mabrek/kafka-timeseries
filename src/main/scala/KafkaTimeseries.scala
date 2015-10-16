import java.nio.charset.StandardCharsets

import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.network.BlockingChannel

import scala.util.control.NonFatal

object KafkaTimeseries {
  def main(args: Array[String]) {
    val topic = args(0)
    val partition = Integer.parseInt(args(1))
    val offset = java.lang.Long.parseLong(args(2))
    val topicAndPartition = new TopicAndPartition(topic, partition)
    val name = "client-" + topic + "-" + partition
    val consumer = new SimpleConsumer("localhost", 9092, 5000, BlockingChannel.UseDefaultBufferSize, name)
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
          val payloadBytes = new Array[Byte](mo.message.payloadSize)
          mo.message.payload.get(payloadBytes)
          System.out.println("key: " + new String(keyBytes, StandardCharsets.UTF_8) + 
            " payload: " + new String(payloadBytes, StandardCharsets.UTF_8))
          Math.max(o, mo.offset)
      }
      System.out.println("next offset " + nextOffset)
    } catch {
      case NonFatal(e) => 
        System.out.println("oops: " + e)
        e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
}
