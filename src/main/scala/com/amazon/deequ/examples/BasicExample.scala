package com.amazon.deequ.examples

import ExampleUtils.withSpark
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.checks.CheckStatus._
import com.amazon.deequ.constraints.ConstraintStatus

object BasicExample extends App {

  case class Item(id: Long, name: String, description: String, priority: String, numViews: Long)

  withSpark { session =>

    val rdd = session.sparkContext.parallelize(Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12)))

    val data = session.createDataFrame(rdd)

    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
          // 'id' should never be NULL
          .isComplete("id")
          // 'id' should not contain duplicates
          .isUnique("id")
          // 'name' should never be NULL
          .isComplete("name")
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

    if (verificationResult.status == Success) {
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
}
