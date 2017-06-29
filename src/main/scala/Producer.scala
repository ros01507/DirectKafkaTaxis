import java.util._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
object Producer {

  def main(args: Array[String]) {
    val kafkaBrokers = "localhost:2181"

    val kafkaOpTopic = "test"
    /*val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")*/

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:2181")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val spark : SparkSession = SparkSession.builder()
      .appName("Smack_Kafka_Spark")
      .master("local[*]")
      .getOrCreate()

    val textFile: sql.Dataset[String] = spark.read.textFile("/home/ros01507/apps/Proyecto/yellow/yellow_min.csv")
    textFile.foreach(record => {
      val data = record.toString
      val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
      producer.send(message)
    })
    producer.close()
  }
}