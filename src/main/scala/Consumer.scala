// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Consumer {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "Consumer", Seconds(1))

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")

    // List of topics you want to listen for from Kafka
    val topics = List("amarillo").toSet

    // Create our Kafka stream, which will contain (topic,message) pairs. We tack a
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)
    println(lines)

    // Extract the request field from each log line
 //   val requests = lines.flatMap(_.split(","))

  //  requests.count().print()


    // Kick it off

    ssc.start()
    ssc.awaitTermination()
  }
}