package com.utad.bigdata.sparkdemo


import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.Stream.Empty

object SaveHdfsCassandra {
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

    val sparkConf = new SparkConf().setAppName("SaveParquet").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topics = "yellow"
    val topicsSet = topics.split(",").toSet
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val words = stream.map(record => (record.key, record.value)).flatMap(_._2.split("\n"))


    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { rdd =>

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      val connector = CassandraConnector.apply(spark.sparkContext.getConf)

      connector.withSessionDo { session =>
        Statements.createKeySpaceAndTable(session, true)
      }
      import spark.implicits._
      import org.apache.spark.sql.functions._
      val words = rdd.map(_.split(",")).filter(row => row(4) != "trip_distance")

      val df = words.map { case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16) =>
        Record(s0.toInt, s1, s2, s3.toInt, s4.toDouble, s5.toInt, s6, s7.toInt, s8.toInt, s9.toInt, s10.toDouble, s11.toDouble, s12.toDouble, s13.toDouble,
          s14.toDouble, s15.toDouble, s16.toDouble)
      }.toDF()

     // val reg = df.count()
     // println(reg)

      if (reg != Empty) {

        // Persistimos en hdfs
        df.write.mode(SaveMode.Append).parquet("hdfs://localhost:9000/user/Taxis/yellow/");

        // Escritura en tabla yellow_tripdata con todos los datos
        df.write.format("org.apache.spark.sql.cassandra")
          .mode(SaveMode.Append)
          .options(Map("keyspace" -> "taxisny", "table" -> "yellow_tripdata"))
          .save()
        val count_yellow_tripdata = df.count()
        println(s"Succesfully saved $count_yellow_tripdata")
        df.show()

        // Agregacion cubo 1: Media total_amount y sum payment type por pickup datetime y passenger count
        val cubo_query1 = df.groupBy("tpep_pickup_datetime", "payment_type", "ratecodeid")
          .agg(avg("total_amount"), sum("passenger_count"))

        // Agregacion cubo 2: sum de trip_distance y avg total_amount por pickup datetime y total amount.
        val cubo_query2 = df.groupBy("tpep_pickup_datetime", "pulocationid", "ratecodeid")
          .agg(sum("trip_distance"), avg("total_amount"))


        // Escribimos en cassandra cubo 1
        cubo_query1.write.format("org.apache.spark.sql.cassandra")
          .mode(SaveMode.Append)
          .options(Map("keyspace" -> "taxisny", "table" -> "avg_paymenttype_por_rateid"))
          .save()
        val count_avg_paymentType_por_rateid = cubo_query1.count()
        println(s"Succesfully saved $count_avg_paymentType_por_rateid")
        cubo_query1.show()

        // Escribimos en cassandra cubo 2

        cubo_query2.write.format("org.apache.spark.sql.cassandra")
          .mode(SaveMode.Append)
          .options(Map("keyspace" -> "taxisny", "table" -> "sum_trip_distance_por_pulocationid"))
          .save()
        val count_sum_trip_distance_por_pulocationid = cubo_query1.count()
        println(s"Succesfully saved $count_sum_trip_distance_por_pulocationid")
        cubo_query2.show()
      }

    }

    ssc.start()
    ssc.awaitTermination()

  }

  case class Record(vendorid: Int,
                    tpep_pickup_datetime: String,
                    tpep_dropoff_datetime: String,
                    passenger_count: Int,
                    trip_distance: Double,
                    ratecodeid: Int,
                    store_and_fwd_flag: String,
                    pulocationid: Int,
                    dolocationid: Int,
                    payment_type: Int,
                    fare_amount: Double,
                    extra: Double,
                    mta_tax: Double,
                    tip_amount: Double,
                    tolls_amount: Double,
                    improvement_surcharge: Double,
                    total_amount: Double)

}


