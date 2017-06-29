import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

object Direct010KafkaSaveParquet {

  def main(args: Array[String]) {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-group-id",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaSaveParquet").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topics = Array("amarillo", "azul")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val lines = stream.map(record => (record.key, record.value))
    val words = lines.flatMap(_._2.split("\n"))

    words.count().print()

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD((rdd: RDD[String], time: Time) => {

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()
      wordsDataFrame.write.mode(SaveMode.Append).parquet("/tmp/parquet");
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }

  case class Record(word: String)

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

}

