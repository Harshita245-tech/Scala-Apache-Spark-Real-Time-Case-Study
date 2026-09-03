import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD

object StreamingProcessor {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("BookingPaymentStreaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // ---------------------------------------------------------
    // CHECKPOINT
    // ---------------------------------------------------------
    ssc.checkpoint("data/checkpoint")

    // ---------------------------------------------------------
    // INPUT DIRECTORY
    // ---------------------------------------------------------
    val inputDirectory = "data/stream"

    val lines = ssc.textFileStream(inputDirectory)

    // ---------------------------------------------------------
    // PARSE BOOKING RECORDS
    // ---------------------------------------------------------
    val bookings = lines
      .filter(line => !line.toLowerCase.startsWith("booking_id"))
      .flatMap { line =>

        val parts = line.split(",", -1).map(_.trim)

        if (parts.length >= 7) {

          try {

            Some(
              (
                parts(0),                 // booking_id
                parts(1),                 // customer_id
                parts(2),                 // booking_type
                parts(3),                 // service_id
                parts(4),                 // booking_date
                parts(5).toDouble,        // amount
                parts(6)                  // status
              )
            )

          } catch {
            case _: Exception => None
          }

        } else {
          None
        }
      }

    // =========================================================
    // 1. DSTREAM / MICRO-BATCH
    // =========================================================

    bookings.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("======================================================")
        println("             BOOKING PAYMENT PLATFORM")
        println("======================================================")
        println()
        println("1. DSTREAM / MICRO-BATCH")

        val records = rdd.collect()

        println("Records received: " + records.length)

        records.foreach { record =>

          println(
            record._1 + " | " +
            record._2 + " | " +
            record._3 + " | " +
            record._6 + " | " +
            record._7
          )
        }

        // -----------------------------------------------------
        // 2. STATELESS TRANSFORMATION
        // -----------------------------------------------------

        println()
        println("2. STATELESS TRANSFORMATION")

        val confirmedCount =
          rdd
            .filter(record => record._7 == "CONFIRMED")
            .count()

        println("Confirmed bookings: " + confirmedCount)

        // -----------------------------------------------------
        // 3. PAIR RDD - CUSTOMER REVENUE
        // -----------------------------------------------------

        println()
        println("3. PAIR RDD - CUSTOMER REVENUE")

        val customerRevenue: RDD[(String, Double)] =
          rdd
            .filter(record => record._7 == "CONFIRMED")
            .map(record => (record._2, record._6))
            .reduceByKey(_ + _)

        customerRevenue
          .collect()
          .sortBy(_._1)
          .foreach {
            case (customer, revenue) =>
              println(customer + " -> " + revenue)
          }

        // -----------------------------------------------------
        // 4. BOOKING TYPE REVENUE
        // -----------------------------------------------------

        println()
        println("4. BOOKING TYPE REVENUE")

        val bookingTypeRevenue: RDD[(String, Double)] =
          rdd
            .filter(record => record._7 == "CONFIRMED")
            .map(record => (record._3, record._6))
            .reduceByKey(_ + _)

        bookingTypeRevenue
          .collect()
          .sortBy(_._1)
          .foreach {
            case (bookingType, revenue) =>
              println(bookingType + " -> " + revenue)
          }

        println()
        println("======================================================")
        println("             END OF CURRENT BATCH")
        println("======================================================")
      }
    }

    // =========================================================
    // 5. STATEFUL CUSTOMER REVENUE
    // =========================================================

    val customerRevenueStream: org.apache.spark.streaming.dstream.DStream[
      (String, Double)
    ] =
      bookings
        .filter(record => record._7 == "CONFIRMED")
        .map(record => (record._2, record._6))

    val updateCustomerRevenue =
      (values: Seq[Double], previous: Option[Double]) => {

        val currentTotal = values.sum

        Some(
          currentTotal + previous.getOrElse(0.0)
        )
      }

    val statefulCustomerRevenue =
      customerRevenueStream.updateStateByKey[Double](
        updateCustomerRevenue
      )

    statefulCustomerRevenue.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("5. STATEFUL CUSTOMER REVENUE")

        rdd
          .collect()
          .sortBy(_._1)
          .foreach {
            case (customer, revenue) =>
              println(customer + " -> " + revenue)
          }
      }
    }

    // =========================================================
    // 6. 15 SECOND WINDOW REVENUE
    // =========================================================

    val windowRevenue =
      customerRevenueStream
        .map {
          case (customer, amount) =>
            ("ALL", amount)
        }
        .reduceByKeyAndWindow(
          (a: Double, b: Double) => a + b,
          Seconds(15),
          Seconds(5)
        )

    // Booking type window revenue separately
    val bookingTypeRevenueStream =
      bookings
        .filter(record => record._7 == "CONFIRMED")
        .map(record => (record._3, record._6))

    val windowBookingTypeRevenue =
      bookingTypeRevenueStream.reduceByKeyAndWindow(
        (a: Double, b: Double) => a + b,
        Seconds(15),
        Seconds(5)
      )

    windowBookingTypeRevenue.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("6. 15 SECOND WINDOW REVENUE")

        rdd
          .collect()
          .sortBy(_._1)
          .foreach {
            case (bookingType, revenue) =>
              println(bookingType + " -> " + revenue)
          }
      }
    }

    // =========================================================
    // 7. 15 SECOND WINDOW BOOKING COUNT
    // =========================================================

    val bookingTypeCountStream =
      bookings
        .filter(record => record._7 == "CONFIRMED")
        .map(record => (record._3, 1))

    val windowBookingTypeCount =
      bookingTypeCountStream.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(15),
        Seconds(5)
      )

    windowBookingTypeCount.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("7. 15 SECOND WINDOW BOOKING COUNT")

        rdd
          .collect()
          .sortBy(_._1)
          .foreach {
            case (bookingType, count) =>
              println(bookingType + " -> " + count)
          }

        println()
        println("======================================================")
        println("             STREAMING BATCH COMPLETE")
        println("======================================================")
      }
    }

    // ---------------------------------------------------------
    // START STREAMING
    // ---------------------------------------------------------

    println()
    println("======================================================")
    println("       BOOKING PAYMENT STREAMING STARTED")
    println("======================================================")
    println("Batch interval : 5 seconds")
    println("Window         : 15 seconds")
    println("Input          : " + inputDirectory)
    println("======================================================")
    println()

    ssc.start()
    ssc.awaitTermination()
  }
}
