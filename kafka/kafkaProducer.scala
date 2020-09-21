package kafka

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.StdIn

object kafkaProducer {

  val kafkaTopic = "InputKStreamTopic"
  val kafkaBootstrapServer = "192.168.1.131:32770,192.168.1.131:32768"

  def getProducer() = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MyKafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }


  def putRecords(): Unit = {

    val kafkaProducer = getProducer()
    // create a record to send to kafka
    val record = new ProducerRecord[String, String](kafkaTopic, "Key01", s"SCALA001")
    kafkaProducer.send(record)
    kafkaProducer.flush()

    kafkaProducer.close()
  }

  def putRecordsLoop(): Unit = {
    val producer = getProducer()
    try {

      for (i <- 0 to 5) {
        val record = new ProducerRecord[String, String](kafkaTopic, "IT" + i, "Message From Scala " + i)
        producer.send(record)
      }

      for (i <- 0 to 5) {
        val record = new ProducerRecord[String, String](kafkaTopic, "COMP" + i, "Message From Scala " + i)
        producer.send(record)
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
  }

  def readFromLine(): Unit = {
    val producer = getProducer()
    // Until stopped or connection broken continue reading
    var count =1
    while (count<=10) {
      val key :String = StdIn.readLine("Input Value of key ")
      val value :String = StdIn.readLine("Input Value of value ")

      val record = new ProducerRecord[String, String](kafkaTopic, s"$key", s"$value" )
      producer.send(record)


      count +=1
    }
  }


  def main(args: Array[String]): Unit = {
//    putRecords()
//    putRecordsLoop()
    readFromLine()
  }


}
