package kafka

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{Grouped, KStream, KTable, Materialized, Serialized}

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.immutable.Stream.StreamBuilder

object KafkaStreams extends App {

  val config: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamApp")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.131:32770")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties
  }
  val inputTopic = "InputKStreamTopic"
  val outputTopic = "OutputKStreamTopic"

  //StreamsBuilder provide the high-level Kafka Streams DSL domain-specific language
  //to specify a Kafka Streams topology.
  val builder: StreamsBuilder = new StreamsBuilder

  //  Creates a KStream from the specified topics.
  val inputStream: KStream[String, String] = builder.stream(inputTopic)

  //Store the input stream to the output topic.
  inputStream.to(outputTopic)

  //Starts the Streams Application
  val kEventStream = new KafkaStreams(builder.build(), config)
    kEventStream.start()
    sys.ShutdownHookThread {
      kEventStream.close(Duration.ofMinutes(20))
    }

}