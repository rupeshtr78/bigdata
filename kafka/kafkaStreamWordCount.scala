package kafka

import java.time.Duration
import java.util.Properties


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes

import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized, Printed, Produced}


import scala.collection.JavaConverters.asJavaIterableConverter



object kafkaStreamWordCount extends App {

  val config: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamApp")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.200:9092")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
    properties
  }
  val inputTopic = "InputKStreamTopic"
  val outputTopic = "OutputKStreamTopic"


  //  StreamsBuilder provide the high-level Kafka Streams DSL domain-specific language to specify a Kafka Streams topology.
  val builder: StreamsBuilder = new StreamsBuilder

//  State full Operations@Todo
  val textLines: KStream[String, String] = builder.stream[String, String](inputTopic)
  val wordKStream: KStream[String, String] = textLines
    .flatMapValues{ textLine =>
      println(s"Textline=  $textLine")
      textLine.toLowerCase.split("\\W+").toIterable.asJava
       }

    wordKStream.print(Printed.toSysOut())

  val wordsMap: KStream[String, String] = wordKStream.map((_,value) => new KeyValue[String,String](value,value))
  wordsMap.print(Printed.toSysOut())

  val wordsMapGKeyTable: KTable[String,String]  = wordsMap.groupByKey().count() .mapValues(value => value.toString())

  val wordsCountKStream: KStream[String,String] = wordsMapGKeyTable.toStream()
  wordsCountKStream.print(Printed.toSysOut())


//  wordsCountKStream.peek((k,v) =>
//  {
//    val theKey = k
//    val theValue =v
//  })
//    .to(outputTopic)

  val stringSerde = Serdes.String
  val longSerde = Serdes.Long()

  wordsCountKStream.to(outputTopic)
//    Produced.`with`(stringSerde,longSerde))


  //  Start the Streams Application
  val kEventStream = new KafkaStreams(builder.build(), config)
  kEventStream.start()
  sys.ShutdownHookThread {
    kEventStream.close(Duration.ofMinutes(20))
  }
}
