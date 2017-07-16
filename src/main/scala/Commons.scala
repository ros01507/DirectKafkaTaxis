

object Commons {

  case class yellow_tripdata(VendorID: String,
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
    extends Serializable

 /* def getTimeStamp(timeStr: String): Timestamp = {
    val dateFormat1: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val date: Option[Timestamp] = {
      try {
        Some(new Timestamp(dateFormat1.parse(timeStr).getTime))
      } catch {
        case e: java.text.ParseException =>
          Some(new Timestamp(dateFormat2.parse(timeStr).getTime))
      }
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }*/

}