import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import java.net.InetAddress

object Direct010 {
  def main(args: Array[String]) {

    // Reducimos el nivel del logger
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-group-id",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> "false"
  )

  // Create context with 2 second batch interval
  val sparkConf = new SparkConf().setAppName("Direct010").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(20))
  val topics = "amarillo,azul"
  val topicsSet = topics.split(",").toSet
    println(topicsSet)
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topicsSet, kafkaParams)
  )

    stream.map(record => (record.key, record.value))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.mapPartitions { iter =>
        val osr: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        val host = InetAddress.getLocalHost().getHostName()
        val count = iter.size
        Seq(s"${host} ${osr.topic} ${osr.partition} ${count}").toIterator
      }.collect.sorted.foreach(println)
    }



    // Start the computation
  ssc.start()
  ssc.awaitTermination()

  }


}