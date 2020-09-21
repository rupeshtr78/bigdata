package kafka

import java.time.Duration
import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition


object KafkaConsumer {

  def main(args: Array[String]): Unit = {
    val topics = List("OutputKStreamTopic")
    consumeKafka(topics)
  }


  def consumeKafka(topics:List[String]) = {


    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.200:32795,192.168.1.200:32798")
    props.put(ConsumerConfig.GROUP_ID_CONFIG,"consgroup01")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "latest")
//    props.put("group.id", "consgroup")
//    props.put("bootstrap.servers", "192.168.1.180:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")



    val consumer = new KafkaConsumer(props)
//    consumer.assign(TopicPartition(topics, 1))

    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(Duration.ofMillis(10))
      for (record <- records.asScala) {
        println("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition() +
          ", TimeStamp: " + record.timestamp())
      }

    }

    consumer.close()
//    consumer.close(Duration.ofMinutes(60))
//    Thread.sleep(1000000)
}

}


