package com.amazon.deequ.demo

import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.suggestions.rules.UniqueIfApproximatelyUniqueRule
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

import DemoUtils._

object ConstraintSuggestion extends App {

  withSpark { session =>

    val data = readCSVFile("01", session)

    val suggestionResult = ConstraintSuggestionRunner()
      .onData(data)
      .addConstraintRules(Rules.DEFAULT ++ Seq(UniqueIfApproximatelyUniqueRule()))
      .useTrainTestSplitWithTestsetRatio(0.4)
      .run()

    val metrics = suggestionResult.verificationResult.get.metrics

    suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
      suggestions.foreach { suggestion =>

        val resultOnTestSet = suggestion.constraint.evaluate(metrics)

        val evalMessage = if (resultOnTestSet.status == ConstraintStatus.Success) {
          "satisfied on holdout set"
        } else {
          s"failed on holdout set, value was ${resultOnTestSet.metric.get}"
        }

        println(
          s"""Constraint suggestion for '$column':
              | Constraint: ${suggestion.description}
              | Code:       ${suggestion.codeForConstraint}
              | Hold-out:   $evalMessage
             """.stripMargin)
      }
    }

  }
}
