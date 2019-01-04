package com.amazon.deequ.demo

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.ConstraintStatus
import DemoUtils._

object DataUnitTest extends App {

  withSpark { session =>

    val data = readCSVFile("03", session)

    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(ReviewChecks.SUGGESTED_CHECK)
      .addCheck(ReviewChecks.CUSTOM_CHECK)
      .addCheck(ReviewChecks.createCardinalityCheck("03"))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
    }

  }
}
