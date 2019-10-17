package com.amazon.deequ.examples

import ExampleUtils.withSpark
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.spark.sql.types.DoubleType
private[examples] object PengExample extends App {
  val t1 = System.nanoTime
  withSpark { session =>

    val data = session.read.format("csv")
      .option("header", "true")
      .load("/Users/chnpng/Downloads/10_m_Trips.csv")

    val df = data.withColumn("Tips", data("Tips").cast(DoubleType))

    val verificationResult = VerificationSuite()
      .onData(df)
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
          // we expect 5 records
          .hasSize(_ == 5)
          // 'id' should never be NULL
          .isComplete("Trip ID")
          // 'id' should not contain duplicates
          .isUnique("Trip ID")
          // 'productName' should never be NULLwh
          .isComplete("Trip Seconds")
          // 'priority' should only contain the values "high" and "low"
          .isContainedIn("Payment Type", Array("Cash", "Credit Card"))
          // 'numViews' should not contain negative values
          .isNonNegative("Fare"))
      .addCheck(
        Check(CheckLevel.Warning, "distribution checks")
          // at least half of the 'description's should contain a url
          .containsURL("Company", _ >= 0.5)
          // half of the items should have less than 10 'numViews'
          .hasApproxQuantile("Tips", 0.5, _ <= 2))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data, the following constraints were not satisfied:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result =>
          println(s"${result.constraint} failed: ${result.message.get}")
        }
    }

  }
  val duration = (System.nanoTime - t1) / 1e9d
  println(s"duration is ${duration}")
}
