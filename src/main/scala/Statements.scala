package com.utad.bigdata.sparkdemo

import com.datastax.driver.core.Session

object Statements extends Serializable {

  def cql(vendorid: String,
          tpep_pickup_datetime: String,
          tpep_dropoff_datetime: String,
          passenger_count: String,
          trip_distance: String,
          ratecodeid: String,
          store_and_fwd_flag: String,
          pulocationid: String,
          dolocationid: String,
          payment_type: String,
          fare_amount: String,
          extra: String,
          mta_tax: String,
          tip_amount: String,
          tolls_amount: String,
          improvement_surcharge: String,
          total_amount: String): String =
    s"""
       insert into taxisny.yellow_tripdata (vendorid,tpep_pickup_datetime,tpep_dropoff_datetime,
       passenger_count,trip_distance,ratecodeid,store_and_fwd_flag,pulocationid,dolocationid,payment_type,
       fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount)
       values('$vendorid', '$tpep_pickup_datetime', '$tpep_dropoff_datetime', '$passenger_count',
        '$trip_distance', '$ratecodeid', '$store_and_fwd_flag', '$pulocationid', '$dolocationid',
         '$payment_type', '$fare_amount', '$extra', '$mta_tax', '$tip_amount', '$tolls_amount',
          '$improvement_surcharge', '$total_amount')"""

  def createKeySpaceAndTable(session: Session, dropTable: Boolean = false) = {
    session.execute(
      """CREATE KEYSPACE  if not exists taxisny WITH REPLICATION =
        | { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""".stripMargin)
    //  if (dropTable)
    //session.execute("""drop table if exists taxisny.yellow_tripdata""")

  /*      |trip_distance double,ratecodeid int,store_and_fwd_flag text,pulocationID int,dolocationid int,
        |payment_type int, fare_amount int,extra double,mta_tax double,tip_amount double,tolls_amount double,
        |improvement_surcharge double,total_amount double, primary key ((total_amount),tpep_pickup_datetime))
        |WITH CLUSTERING ORDER BY (tpep_pickup_datetime DESC)""".stripMargin)  */

    session.execute(
      """create table if not exists taxisny.avg_paymenttype_por_rateid
        |(tpep_pickup_datetime timestamp,payment_type int,ratecodeid int,total_amount double,
        |passenger_count int, "avg(total_amount)" double, "sum(passenger_count)" int, primary key ((tpep_pickup_datetime),payment_type,ratecodeid))
        |WITH CLUSTERING ORDER BY (payment_type DESC)""".stripMargin)

    session.execute(
      """create table if not exists taxisny.sum_trip_distance_por_pulocationid
        |(tpep_pickup_datetime timestamp,pulocationid int,ratecodeid int,trip_distance double,
        |total_amount double, "sum(trip_distance)" double, "avg(total_amount)" double, primary key ((tpep_pickup_datetime),pulocationid,ratecodeid))
        |WITH CLUSTERING ORDER BY (pulocationid DESC)""".stripMargin)


  }}
