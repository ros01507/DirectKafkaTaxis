import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SchemaParquetFile {
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


    val sparkConf = new SparkConf().setAppName("SaveParquet").setMaster("local[2]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(sparkConf)
   // val sqlContext = new SparkSession(sc)
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topics = "yellow"
    val topicsSet = topics.split(",").toSet
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    val words = stream.map(record => (record.key, record.value)).flatMap(x => x._2.split("\n"))

    val schemaString = "VendorID tpep_pickup_datetime tpep_dropoff_datetime passenger_count trip_distance RatecodeID store_and_fwd_flag PULocationID DOLocationID payment_type fare_amount extra mta_tax tip_amount tolls_amount improvement_surcharge total_amount"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { rdd =>

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.sqlContext


      val rowRDD = rdd.map(s ⇒ Row(s(0), s(1), s(2), s(3), s(4),
        s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13),
        s(14), s(15), s(16)))

      println(s"$rowRDD")

      val wordsDF = sqlContext.createDataFrame(rowRDD, schema)

      wordsDF.printSchema()
      wordsDF.write.mode(SaveMode.Append).parquet("/tmp/parquet");

    }

    ssc.start()
    ssc.awaitTermination()

  }

}

object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}