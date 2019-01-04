package com.amazon.deequ.demo

import DemoUtils._
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.InMemoryStateProvider
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.ConstraintStatus

object IncrementalDataUnitTest extends App {

  val days = Array("01", "02", "03", "04", "05", "06", "07")

  var lastState = InMemoryStateProvider()

  withSpark { session =>

    days.foreach { day =>

      val nextState = InMemoryStateProvider()

      val checksToApply = Seq(
        ReviewChecks.SUGGESTED_CHECK,
        ReviewChecks.CUSTOM_CHECK,
        ReviewChecks.createCardinalityCheck(day))

      val verificationResult = VerificationSuite().run(
        data = readCSVFile(day, session),
        checks = checksToApply,
        aggregateWith = Some(lastState),
        saveStatesWith = Some(nextState)
      )

      if (verificationResult.status == CheckStatus.Success) {
        println(s"Day $day: the data passed the test, everything is fine!")
      } else {
        println(s"Day $day: we found errors in the data:\n")

        val resultsForAllConstraints = verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }

        resultsForAllConstraints
          .filter { _.status != ConstraintStatus.Success }
          .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
      }

      lastState = nextState
    }
  }

}
