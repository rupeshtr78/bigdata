package kafka

import java.text.SimpleDateFormat
import java.sql.{Date, Timestamp}
import java.util.Date

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}
import org.apache.spark.sql.cassandra._



object SparkKafkaStream {

  val spark = SparkSession.builder()
    .appName("Spark Kafka Stream")
    .master("local[*]")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .config("spark.cassandra.connection.host","192.168.1.200")
    .config("spark.cassandra.connection.port","9042")
    .config("spark.cassandra.auth.username","cassandra")
    .config("spark.cassandra.auth.password","cassandra")
    .getOrCreate()

  def readKafka() ={
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.181:9092")
      //      .option("subscribe","streams-spark-input")
      .option("assign", "{\"streams-spark-input3\":[0]}")
      .load()

  }




  val regexPatterns = Map(
    "ddd" -> "\\d{1,3}".r,
    "ip" -> """s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"""".r,
    "client" -> "(\\S+)".r,
    "user" -> "(\\S+)".r,
    "dateTime" -> "(\\[.+?\\])".r,
    "datetimeNoBrackets" -> "(?<=\\[).+?(?=\\])".r,
    "request" -> "\"(.*?)\"".r,
    "status" -> "(\\d{3})".r,
    "bytes" -> "(\\S+)".r,
    "referer" -> "\"(.*?)\"".r,
    "agent" -> """\"(.*)\"""".r
  )

//  "agent" -> "\"(.*?)\"".r

  def parseLog(regExPattern: String) = udf((url: String) =>
       regexPatterns(regExPattern).findFirstIn(url) match
          {
      case Some(parsedValue) => parsedValue
      case None => "unknown"
       }
  )


  //  val requestRegex = "(\\S+)"

  import spark.implicits._

  def parseKafka() ={
    readKafka()
      .select(col("topic").cast(StringType),
              col("offset"),
              col("value").cast(StringType))
//      .withColumn("client",regexp_extract(col("value"),requestRegex,0))
      .withColumn("user", parseLog("user")($"value"))
      .withColumn("dateTime", parseLog("datetimeNoBrackets")($"value"))
      .withColumn("request", parseLog("request")($"value"))
      .withColumn("agent", parseLog("agent")($"value"))
      .withColumn("status", parseLog("status")($"agent"))

//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .start()
//      .awaitTermination()

  }



  case class logsDate(offset: BigInt,
                      status: String,
                      user: String,
                      request: String,
                      dateTime: Timestamp)

  val DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss ZZZZ"
  val dateFormat = new SimpleDateFormat(DATE_FORMAT)

  val  convertStringToDate = udf((dateString: String) =>
    dateFormat.parse(dateString).getTime
  )

  val  convertStringToDate2 = udf((dateString: String) =>
    new Timestamp(dateFormat.parse(dateString).getTime)
  )



  def parseDate() = {
    parseKafka().select(col("offset"), col("status"),
      col("user"),col("request"),
      col("dateTime"))
//      to_timestamp(col("dateTime"),"dd/MMM/yyyy:HH:mm:ss ZZZZ").as("to_date"))
      .withColumn("dateTime", convertStringToDate2(col("dateTime")))
      .as[logsDate]
//      .printSchema()      .writeStream
    ////      .format("console")
    ////      .outputMode("append")
    ////      .start()
    ////      .awaitTermination()
//
}

  def writeToKafka(): Unit ={
    parseDate().select(
      col("offset").cast("String").as("key"),
      to_json(struct(col("*"))).as("value")
    )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.181:9092")
      .option("topic","streams-spark-output")
      .option("checkpointLocation","checkpoint")
      .start()
      .awaitTermination()
  }


  def urlRegEx(): Unit ={
    parseKafka().select(col("agent"))
      .withColumn("status",parseLog("status")($"agent"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def writeToCassandra(): Unit = {
    parseDate().select(col("offset"), col("status"),
      col("user"),col("request"),
      col("dateTime"))
      .as[logsDate]
      .writeStream
      .foreachBatch { (batch: Dataset[logsDate], batchId: Long) =>
        batch.select(col("*"))
          .write
          .cassandraFormat("kafkalogsdate", "hyper")
          .mode(SaveMode.Append)
          .save()
      }

      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    readKafka()
      .writeStream
      .format("console")
      .start()
      .awaitTermination()


  }

}


//create table hyper.kafkalogsdate("offset" text primary key , "status" text, "user" text,"request"  text ,"dateTime" timestamp  );