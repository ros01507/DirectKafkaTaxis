
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Direct010 {
  def main(args: Array[String]) {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-group-id",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  // Create context with 2 second batch interval
  val sparkConf = new SparkConf().setAppName("Direct010").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  val topics = Array("amarillo", "yellow")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

    val lines = stream.map(record => (record.key, record.value))

  // Get the lines, split them into words, count the words and print

  val words = lines.flatMap(_._2.split(" "))

  //val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  words.count().print()

  // Convert RDDs of the words DStream to DataFrame and run SQL query

  // Start the computation
  ssc.start()
  ssc.awaitTermination()

  }}