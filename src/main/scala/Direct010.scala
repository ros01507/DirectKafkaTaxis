import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Direct010 {
  def main(args: Array[String]) {

    // Reducimos el nivel del logger
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-consumer-group",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val kafkaBrokers = "localhost:9092"
  // Create context with 2 second batch interval

  val sparkConf = new SparkConf().setAppName("Direct010").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val topics = "yellow"
  val topicsSet = topics.split(",").toSet
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topicsSet, kafkaParams)
  )
  val str = stream.map(record => (record.key, record.value)).flatMap(x => x._2.split("\n"))

  str.foreachRDD {rdd => rdd.foreach {line => println(s"$line")}}
 //str.foreachRDD {rdd => rdd.foreach(println)}

    // Start the computation
  ssc.start()
  ssc.awaitTermination()

  }

}

