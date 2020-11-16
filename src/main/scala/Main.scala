import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    withSpark { spark =>
      import spark.implicits._

      val data = Seq(Item(1, "Thingy A", "awesome thing.", "high", 0),
        Item(2, "Thingy B", "available at http://thingb.com", null, 0),
        Item(3, null, null, "low", 5),
        Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        Item(5, "Thingy E", null, "high", 12)).toDF()

      data.schema
      data.show()

      val verificationResult = VerificationSuite()
        .onData(data)
        .addCheck(
          Check(CheckLevel.Error, "integrity checks")
            // we expect 5 records
            .hasSize(_ == 5)
            // 'id' should never be NULL
            .isComplete("id")
            // 'id' should not contain duplicates
            .isUnique("id")
            // 'productName' should never be NULL
            .isComplete("productName")
            // 'priority' should only contain the values "high" and "low"
            .isContainedIn("priority", Array("high", "low"))
            // 'numViews' should not contain negative values
            .isNonNegative("numViews"))
        .addCheck(
          Check(CheckLevel.Warning, "distribution checks")
            // at least half of the 'description's should contain a url
            .containsURL("description", _ >= 0.5)
            // half of the items should have less than 10 'numViews'
            .hasApproxQuantile("numViews", 0.5, _ <= 10))
        .run()

      if (verificationResult.status == CheckStatus.Success) {
        println("The data passed the test, everything is fine!")
      } else {
        println("We found errors in the data, the following constraints were not satisfied:\n")

        val resultsForAllConstraints = verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }

        resultsForAllConstraints
          .filter {
            _.status != ConstraintStatus.Success
          }
          .foreach { result =>
            println(s"${result.constraint} failed: ${result.message.get}")
          }

      }
    }
  }

  def initSpark(): SparkSession = {
    disableSparkLogs()

    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1") // for local demo
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  def disableSparkLogs(): Unit = {
    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def withSpark(func: SparkSession => Unit): Unit = {
    val spark = initSpark()
    spark.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(spark)
    } finally {
      spark.stop()
      System.clearProperty("spark.driver.port")
    }
  }

  case class Item(
                   id: Long,
                   productName: String,
                   description: String,
                   priority: String,
                   numViews: Long
                 )

}
