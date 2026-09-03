import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object BatchProcessor {

  def main(args: Array[String]): Unit = {

    // ==========================================
    // SPARK SESSION
    // ==========================================

    val spark = SparkSession.builder()
      .appName("Booking Payment Platform")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    // ==========================================
    // LOAD DATA
    // ==========================================

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/customers.csv")

    val bookingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/bookings.csv")

    val paymentsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/payments.csv")

    val cancellationsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/cancellations.csv")

    val hotelsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/reference/hotels.csv")

    val flightsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/reference/flights.csv")

    val busesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/reference/buses.csv")


    // ==========================================
    // RECORD COUNTS
    // ==========================================

    println("\n=== DATASET RECORD COUNTS ===")

    println(s"Customers      : ${customersDF.count()}")
    println(s"Bookings       : ${bookingsDF.count()}")
    println(s"Payments       : ${paymentsDF.count()}")
    println(s"Cancellations  : ${cancellationsDF.count()}")
    println(s"Hotels         : ${hotelsDF.count()}")
    println(s"Flights        : ${flightsDF.count()}")
    println(s"Buses          : ${busesDF.count()}")


    // ==========================================
    // SAMPLE DATA
    // ==========================================

    println("\n=== CUSTOMERS ===")
    customersDF.show(5, truncate = false)

    println("\n=== BOOKINGS ===")
    bookingsDF.show(5, truncate = false)

    println("\n=== PAYMENTS ===")
    paymentsDF.show(5, truncate = false)

    println("\n=== CANCELLATIONS ===")
    cancellationsDF.show(5, truncate = false)

    println("\n=== HOTELS ===")
    hotelsDF.show(5, truncate = false)

    println("\n=== FLIGHTS ===")
    flightsDF.show(5, truncate = false)

    println("\n=== BUSES ===")
    busesDF.show(5, truncate = false)


    // ==========================================
    // BOOKING SUMMARY
    // ==========================================

    println("\n=== BOOKING SUMMARY BY TYPE ===")

    val bookingSummary = bookingsDF
      .groupBy("booking_type")
      .agg(
        count("*").alias("total_bookings"),
        sum("amount").alias("total_revenue")
      )
      .orderBy("booking_type")

    bookingSummary.show(false)


    // ==========================================
    // CUSTOMER BOOKING JOIN
    // ==========================================

    println("\n=== CUSTOMER BOOKING ANALYSIS ===")

    val customerBookings = bookingsDF
      .join(
        customersDF,
        bookingsDF("customer_id") === customersDF("customer_id"),
        "inner"
      )
      .select(
        bookingsDF("booking_id"),
        customersDF("customer_id"),
        customersDF("name"),
        customersDF("city"),
        bookingsDF("booking_type"),
        bookingsDF("amount"),
        bookingsDF("status")
      )
      .orderBy("customer_id", "booking_id")

    customerBookings.show(false)


    // ==========================================
    // CUSTOMER SPENDING
    // ==========================================

    println("\n=== CUSTOMER-WISE SPENDING ===")

    val customerSpending = customerBookings
      .groupBy("customer_id", "name")
      .agg(
        count("booking_id").alias("total_bookings"),
        sum("amount").alias("total_spending")
      )
      .orderBy(desc("total_spending"))

    customerSpending.show(false)


    // ==========================================
    // PAYMENT ANALYSIS
    // ==========================================

    println("\n=== PAYMENT ANALYSIS ===")

    val paymentAnalysis = bookingsDF
      .join(
        paymentsDF,
        bookingsDF("booking_id") === paymentsDF("booking_id"),
        "inner"
      )
      .select(
        bookingsDF("booking_id"),
        bookingsDF("customer_id"),
        bookingsDF("booking_type"),
        bookingsDF("amount").alias("booking_amount"),
        bookingsDF("status").alias("booking_status"),
        paymentsDF("payment_method"),
        paymentsDF("payment_amount"),
        paymentsDF("payment_status")
      )
      .orderBy("booking_id")

    paymentAnalysis.show(false)


    // ==========================================
    // PAYMENT SUMMARY
    // ==========================================

    println("\n=== PAYMENT SUMMARY ===")

    val paymentSummary = paymentAnalysis
      .groupBy("payment_method", "payment_status")
      .agg(
        count("booking_id").alias("total_payments"),
        sum("payment_amount").alias("total_amount")
      )
      .orderBy("payment_method", "payment_status")

    paymentSummary.show(false)


    // ==========================================
    // CANCELLATION ANALYSIS
    // ==========================================

    println("\n=== CANCELLATION ANALYSIS ===")

    val cancellationAnalysis = bookingsDF
      .join(
        cancellationsDF,
        bookingsDF("booking_id") === cancellationsDF("booking_id"),
        "inner"
      )
      .select(
        bookingsDF("booking_id"),
        bookingsDF("customer_id"),
        bookingsDF("booking_type"),
        bookingsDF("amount").alias("booking_amount"),
        bookingsDF("status"),
        cancellationsDF("cancellation_date"),
        cancellationsDF("reason"),
        cancellationsDF("refund_amount")
      )
      .orderBy("booking_id")

    cancellationAnalysis.show(false)


    // ==========================================
    // CANCELLATION SUMMARY
    // ==========================================

    println("\n=== CANCELLATION SUMMARY ===")

    val cancellationSummary = cancellationAnalysis
      .groupBy("booking_type", "reason")
      .agg(
        count("booking_id").alias("cancelled_bookings"),
        sum("refund_amount").alias("total_refund")
      )
      .orderBy("booking_type", "reason")

    cancellationSummary.show(false)


    // ==========================================
    // HOTEL ANALYSIS
    // ==========================================

    println("\n=== HOTEL BOOKING ANALYSIS ===")

    val hotelBookings = bookingsDF
      .filter(col("booking_type") === "Hotel")
      .join(
        hotelsDF,
        bookingsDF("service_id") === hotelsDF("hotel_id"),
        "inner"
      )
      .select(
        bookingsDF("booking_id"),
        bookingsDF("customer_id"),
        hotelsDF("hotel_id"),
        hotelsDF("hotel_name"),
        hotelsDF("city").alias("hotel_city"),
        hotelsDF("room_type"),
        hotelsDF("price_per_night"),
        bookingsDF("amount").alias("booking_amount"),
        bookingsDF("status")
      )
      .orderBy("hotel_id")

    hotelBookings.show(false)


    println("\n=== HOTEL SUMMARY ===")

    val hotelSummary = hotelBookings
      .groupBy("hotel_id", "hotel_name", "hotel_city")
      .agg(
        count("booking_id").alias("total_bookings"),
        sum("booking_amount").alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    hotelSummary.show(false)


    // ==========================================
    // FLIGHT ANALYSIS
    // ==========================================

    println("\n=== FLIGHT BOOKING ANALYSIS ===")

    val flightBookings = bookingsDF
      .filter(col("booking_type") === "Flight")
      .join(
        flightsDF,
        bookingsDF("service_id") === flightsDF("flight_id"),
        "inner"
      )
      .select(
        bookingsDF("booking_id"),
        bookingsDF("customer_id"),
        flightsDF("flight_id"),
        flightsDF("airline"),
        flightsDF("source"),
        flightsDF("destination"),
        flightsDF("seat_capacity"),
        bookingsDF("amount").alias("booking_amount"),
        bookingsDF("status")
      )
      .orderBy("flight_id")

    flightBookings.show(false)


    println("\n=== FLIGHT SUMMARY ===")

    val flightSummary = flightBookings
      .groupBy("flight_id", "airline", "source", "destination")
      .agg(
        count("booking_id").alias("total_bookings"),
        sum("booking_amount").alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    flightSummary.show(false)


    // ==========================================
    // BUS ANALYSIS
    // ==========================================

    println("\n=== BUS BOOKING ANALYSIS ===")

    val busBookings = bookingsDF
      .filter(col("booking_type") === "Bus")
      .join(
        busesDF,
        bookingsDF("service_id") === busesDF("bus_id"),
        "inner"
      )
      .select(
        bookingsDF("booking_id"),
        bookingsDF("customer_id"),
        busesDF("bus_id"),
        busesDF("operator"),
        busesDF("source"),
        busesDF("destination"),
        busesDF("seat_capacity"),
        bookingsDF("amount").alias("booking_amount"),
        bookingsDF("status")
      )
      .orderBy("bus_id")

    busBookings.show(false)


    println("\n=== BUS SUMMARY ===")

    val busSummary = busBookings
      .groupBy("bus_id", "operator", "source", "destination")
      .agg(
        count("booking_id").alias("total_bookings"),
        sum("booking_amount").alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    busSummary.show(false)


    // ==========================================
    // PAIR RDD
    // ==========================================

    println("\n=== PAIR RDD CUSTOMER REVENUE ===")

    val customerRevenueRDD = bookingsDF.rdd
      .map { row =>
        val customerId = row.getAs[String]("customer_id")
        val amount = row.getAs[Number]("amount").doubleValue()
        (customerId, amount)
      }
      .reduceByKey(_ + _)
      .sortByKey()

    customerRevenueRDD.collect().foreach {
      case (customerId, revenue) =>
        println(f"$customerId -> $revenue%.2f")
    }


    // ==========================================
    // NARROW TRANSFORMATION
    // ==========================================

    println("\n=== NARROW TRANSFORMATION ===")

    val confirmedBookings = bookingsDF
      .filter(col("status") === "CONFIRMED")

    println(
      s"Total confirmed bookings: ${confirmedBookings.count()}"
    )


    // ==========================================
    // PARTITIONS
    // ==========================================

    println("\n=== PARTITION INFORMATION ===")

    val bookingsRDD = bookingsDF.rdd

    println(
      s"Original partitions: ${bookingsRDD.getNumPartitions}"
    )


    // ==========================================
    // REPARTITION
    // ==========================================

    println("\n=== REPARTITION / SHUFFLE ===")

    val repartitionedBookings =
      bookingsRDD.repartition(4)

    println(
      s"Partitions after repartition: ${repartitionedBookings.getNumPartitions}"
    )

    println(
      s"Records after repartition: ${repartitionedBookings.count()}"
    )


    // ==========================================
    // COALESCE
    // ==========================================

    println("\n=== COALESCE ===")

    val coalescedBookings =
      repartitionedBookings.coalesce(2)

    println(
      s"Partitions after coalesce: ${coalescedBookings.getNumPartitions}"
    )

    println(
      s"Records after coalesce: ${coalescedBookings.count()}"
    )


    // ==========================================
    // WIDE TRANSFORMATION
    // ==========================================

    println("\n=== WIDE TRANSFORMATION ===")

    val bookingTypeRDD = bookingsDF.rdd
      .map { row =>
        val bookingType = row.getAs[String]("booking_type")
        val amount = row.getAs[Number]("amount").doubleValue()
        (bookingType, amount)
      }

    val revenueByTypeRDD =
      bookingTypeRDD.reduceByKey(_ + _)

    revenueByTypeRDD.collect().foreach {
      case (bookingType, revenue) =>
        println(f"$bookingType -> $revenue%.2f")
    }


    // ==========================================
    // CACHE / PERSIST
    // ==========================================

    println("\n=== CACHE / PERSIST ===")

    val confirmedCached = bookingsDF
      .filter(col("status") === "CONFIRMED")
      .persist(StorageLevel.MEMORY_ONLY)

    println(
      s"Cached confirmed bookings: ${confirmedCached.count()}"
    )

    println(
      s"Cached booking count again: ${confirmedCached.count()}"
    )

    confirmedCached.unpersist()

    println("Cache released successfully")


    // ==========================================
    // BROADCAST
    // ==========================================

    println("\n=== BROADCAST VARIABLE ===")

    val hotelMap = hotelsDF
      .collect()
      .map { row =>
        val hotelId = row.getAs[String]("hotel_id")
        val hotelName = row.getAs[String]("hotel_name")
        (hotelId, hotelName)
      }
      .toMap

    val broadcastHotels =
      spark.sparkContext.broadcast(hotelMap)

    println(
      s"Broadcast hotels count: ${broadcastHotels.value.size}"
    )

    val hotelRDD = bookingsDF.rdd
      .filter { row =>
        row.getAs[String]("booking_type") == "Hotel"
      }
      .map { row =>
        val serviceId = row.getAs[String]("service_id")
        val hotelName =
          broadcastHotels.value.getOrElse(serviceId, "Unknown")

        (serviceId, hotelName)
      }

    hotelRDD.collect().foreach {
      case (hotelId, hotelName) =>
        println(s"$hotelId -> $hotelName")
    }

    broadcastHotels.destroy()


    // ==========================================
    // ACCUMULATOR
    // ==========================================

    println("\n=== ACCUMULATOR ===")

    val cancelledBookings =
      spark.sparkContext.longAccumulator("CancelledBookings")

    bookingsDF.rdd.foreach { row =>
      val status = row.getAs[String]("status")

      if (status == "CANCELLED") {
        cancelledBookings.add(1)
      }
    }

    println(
      s"Cancelled bookings counted by accumulator: ${cancelledBookings.value}"
    )


    // ==========================================
    // SPARK SQL
    // ==========================================

    println("\n=== SPARK SQL ===")

    bookingsDF.createOrReplaceTempView("bookings")

    val sqlResult = spark.sql(
      """
        SELECT
          booking_type,
          COUNT(*) AS total_bookings,
          SUM(amount) AS total_revenue,
          AVG(amount) AS average_booking_amount
        FROM bookings
        GROUP BY booking_type
        ORDER BY total_revenue DESC
      """
    )

    sqlResult.show(false)


    // ==========================================
    // SPARK SQL - TOP BOOKINGS
    // ==========================================

    println("\n=== SPARK SQL TOP 5 BOOKINGS ===")

    val topBookingsSQL = spark.sql(
      """
        SELECT
          booking_id,
          customer_id,
          booking_type,
          amount,
          status
        FROM bookings
        ORDER BY amount DESC
        LIMIT 5
      """
    )

    topBookingsSQL.show(false)


    // ==========================================
    // WINDOW FUNCTION
    // ==========================================

    println("\n=== CUSTOMER SPENDING RANK ===")

    val customerWindow =
      Window
        .orderBy(desc("total_spending"))

    val rankedCustomers = customerSpending
      .withColumn(
        "spending_rank",
        rank().over(customerWindow)
      )

    rankedCustomers.show(false)


    // ==========================================
    // CUSTOMER RANK WITH PARTITION
    // ==========================================

    println("\n=== CUSTOMER RANK BY CITY ===")

    val cityWindow =
      Window
        .partitionBy("city")
        .orderBy(desc("amount"))

    val cityCustomerBookings = bookingsDF
      .join(
        customersDF,
        bookingsDF("customer_id") === customersDF("customer_id"),
        "inner"
      )
      .select(
        bookingsDF("booking_id"),
        customersDF("name"),
        customersDF("city"),
        bookingsDF("amount")
      )
      .withColumn(
        "city_rank",
        rank().over(cityWindow)
      )

    cityCustomerBookings.show(false)


    // ==========================================
    // UDF
    // ==========================================

    println("\n=== UDF BOOKING VALUE CLASSIFICATION ===")

    val classifyBooking = udf { amount: Double =>
      if (amount >= 10000) {
        "HIGH VALUE"
      } else if (amount >= 5000) {
        "MEDIUM VALUE"
      } else {
        "LOW VALUE"
      }
    }

    val classifiedBookings = bookingsDF
      .withColumn(
        "booking_category",
        classifyBooking(col("amount").cast("double"))
      )
      .select(
        "booking_id",
        "customer_id",
        "booking_type",
        "amount",
        "booking_category"
      )
      .orderBy(desc("amount"))

    classifiedBookings.show(false)


    // ==========================================
    // RDD LINEAGE
    // ==========================================

    println("\n=== RDD LINEAGE ===")

    val lineageRDD = bookingsDF.rdd
      .filter { row =>
        row.getAs[String]("status") == "CONFIRMED"
      }
      .map { row =>
        (
          row.getAs[String]("customer_id"),
          row.getAs[Number]("amount").doubleValue()
        )
      }

    println("RDD lineage:")
    println(lineageRDD.toDebugString)


    // ==========================================
    // DAG / EXECUTION
    // ==========================================

    println("\n=== DAG / EXECUTION PLAN ===")

    println("DAG is generated when an action is executed.")
    println("Narrow transformations stay within stages.")
    println("Wide transformations create shuffle boundaries.")

    println("\nPhysical plan for customer spending:")

    customerSpending.explain(true)


    // ==========================================
    // FINAL STOP
    // ==========================================

    spark.stop()
  }
}
