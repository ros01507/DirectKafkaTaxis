import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object Direct010SaveParquet {

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
    val sparkConf = new SparkConf().setAppName("Direct010SaveParquet").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topics = Array("prueba")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val words = stream.map(record => (record.key, record.value))
      .flatMap(x => x._2.split("\n"))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { (rdd: RDD[String], time: Time) =>

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val words = rdd.map(w => w.split(","))
      val wordDF = words.map { case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16) =>
        Record(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16)
      }.toDF()
      wordDF.printSchema()
      wordDF.write.mode(SaveMode.Append).parquet("/tmp/parquet");

    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}

/** Case class for converting RDD to DataFrame */
case class Record(VendorID: String,
                  tpep_pickup_datetime: String,
                  tpep_dropoff_datetime: String,
                  passenger_count: String,
                  trip_distance: String,
                  RatecodeID: String,
                  store_and_fwd_flag: String,
                  PULocationID: String,
                  DOLocationID: String,
                  payment_type: String,
                  fare_amount: String,
                  extra: String,
                  mta_tax: String,
                  tip_amount: String,
                  tolls_amount: String,
                  improvement_surcharge: String,
                  total_amount: String)


/** Lazily instantiated singleton instance of SparkSession */
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