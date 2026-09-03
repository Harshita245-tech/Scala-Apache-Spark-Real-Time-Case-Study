import org.apache.spark.sql.SparkSession

case class FinalBooking(
  booking_id: String,
  customer_id: String,
  booking_type: String,
  amount: Double,
  status: String
)

object FinalVerification {

  def main(args: Array[String]): Unit = {

    println("================================================")
    println("       FINAL CASE STUDY VERIFICATION")
    println("================================================")

    // =================================================
    // PART A - SCALA FUNDAMENTALS
    // =================================================

    println("\n1. SCALA FUNDAMENTALS")

    val numbers = Vector(10, 20, 30, 40, 50)
    println("Vector: " + numbers)

    val doubled = for {
      n <- numbers
    } yield n * 2

    println("For-yield result: " + doubled)

    val normalValue = 100
    lazy val lazyValue = {
      println("lazy val evaluated")
      200
    }

    println("val: " + normalValue)
    println("lazy val: " + lazyValue)

    trait BookingRule {
      def ruleName: String
    }

    class PaymentRule extends BookingRule {
      override def ruleName: String = "Payment Validation Rule"
    }

    val rule = new PaymentRule
    println("Trait: " + rule.ruleName)

    // =================================================
    // SPARK SESSION
    // =================================================

    val spark = SparkSession.builder()
      .appName("FinalVerification")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // =================================================
    // PART D - DATASET
    // =================================================

    println("\n2. DATASET")

    val bookings = Seq(
      FinalBooking("BK001", "C001", "Hotel", 10000.0, "CONFIRMED"),
      FinalBooking("BK002", "C002", "Flight", 6500.0, "CONFIRMED"),
      FinalBooking("BK003", "C003", "Bus", 1200.0, "CONFIRMED"),
      FinalBooking("BK004", "C004", "Hotel", 15000.0, "CONFIRMED"),
      FinalBooking("BK005", "C005", "Flight", 7200.0, "CANCELLED")
    )

    val bookingDS = bookings.toDS()

    println("Dataset created successfully")
    println("Dataset count: " + bookingDS.count())

    bookingDS.show(false)

    // =================================================
    // SPARK CATALOG
    // =================================================

    println("\n3. SPARK CATALOG")

    bookingDS.createOrReplaceTempView("final_bookings")

    println("Temporary table created: final_bookings")

    println("Catalog tables:")
    spark.catalog.listTables().show(false)

    println("Catalog database:")
    println(spark.catalog.currentDatabase)

    // =================================================
    // SPARK SQL
    // =================================================

    println("\n4. SPARK SQL")

    spark.sql("""
      SELECT booking_type,
             COUNT(*) AS total_bookings,
             SUM(amount) AS total_revenue
      FROM final_bookings
      GROUP BY booking_type
      ORDER BY total_revenue DESC
    """).show(false)

    // =================================================
    // MONITORING
    // =================================================

    println("\n5. SPARK MONITORING")

    println("Application ID: " + spark.sparkContext.applicationId)
    println("Default parallelism: " + spark.sparkContext.defaultParallelism)
    println("Spark UI: http://localhost:4040")

    // =================================================
    // PRODUCTION / YARN
    // =================================================

    println("\n6. YARN DEPLOYMENT")

    println("Application is developed and tested in local mode.")
    println("For cluster execution, Spark can be submitted using YARN.")
    println("Example deployment mode: --master yarn")

    // =================================================
    // OUTPUT LAYOUT
    // =================================================

    println("\n7. OUTPUT LAYOUT")

    println("Expected project output directory:")
    println("data/output/")
    println("Batch and analysis results can be written here.")

    println("\n================================================")
    println("       FINAL VERIFICATION COMPLETE")
    println("================================================")

    spark.stop()
  }
}
