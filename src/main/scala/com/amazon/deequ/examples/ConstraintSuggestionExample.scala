/** Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  */

package com.amazon.deequ.examples

import com.amazon.deequ.examples.ExampleUtils.withSpark
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.constraints.{
  AnalysisBasedConstraint,
  ConstraintResult,
  ConstraintDecorator,
  Constraint
}

import org.apache.spark.sql.DataFrame

private[examples] object ConstraintSuggestionExample {
  case class RowData(
      productName: String,
      totalNumber: Option[Double],
      status: String,
      valuable: Option[Boolean]
  )

  def main(args: Array[String]): Unit = {
    withSpark { session =>
      // Lets first generate some example data
      val rows = session.sparkContext.parallelize(
        Seq(
          RowData("thingA", Some(13.0), "IN_TRANSIT", Some(true)),
          RowData("thingA", Some(5), "DELAYED", Some(false)),
          RowData("thingB", Some(7.0), "DELAYED", None),
          RowData("thingC", Some(7.1), "IN_TRANSIT", Some(false)),
          RowData("thingD", Some(1.0), "DELAYED", Some(true)),
          RowData("thingC", Some(7.0), "UNKNOWN", None),
          RowData("thingC", Some(24), "UNKNOWN", None),
          RowData("thingE", Some(20), "DELAYED", Some(false)),
          RowData("thingA", Some(13.0), "IN_TRANSIT", Some(true)),
          RowData("thingA", Some(5), "DELAYED", Some(false)),
          RowData("thingB", Some(7.4), "DELAYED", None),
          RowData("thingC", Some(7.9), "IN_TRANSIT", Some(false)),
          RowData("thingD", Some(1.0), "DELAYED", Some(true)),
          RowData("thingC", Some(17.0), "UNKNOWN", None),
          RowData("thingC", Some(22), "UNKNOWN", None),
          RowData("thingE", Some(23), "DELAYED", Some(false))
        )
      )

      val data = session.createDataFrame(rows)
      println(s"SCHEMA: ${data.schema}")

      // We ask deequ to compute constraint suggestions for us on the data
      // It will profile the data and than apply a set of rules specified in addConstraintRules()
      // to suggest constraints
      val suggestionResult = ConstraintSuggestionRunner()
        .onData(data)
        .addConstraintRules(Rules.EXTENDED)
        .run()

      // We can now investigate the constraints that deequ suggested. We get a textual description
      // and the corresponding scala code for each suggested constraint
      //
      // Note that the constraint suggestion is based on heuristic rules and assumes that the data
      // it is shown is 'static' and correct, which might often not be the case in the real world.
      // Therefore the suggestions should always be manually reviewed before being applied in real
      // deployments.
      suggestionResult.constraintSuggestions.foreach {
        case (column, suggestions) =>
          suggestions.foreach { suggestion =>
            println(
              s"Constraint suggestion for '$column':\t${suggestion.description}\n" +
                s"The corresponding scala code is ${suggestion.codeForConstraint}\n"
            )
            val c: Constraint = suggestion.constraint
            val result: ConstraintResult = evalConstraint(c, data)
            println(s"Status: ${result.status} Metric: $result.metric")
          }
      }
    }
  }

  def evalConstraint(
      constraint: Constraint,
      df: DataFrame
  ): ConstraintResult = {

    val analysisBasedConstraint = constraint match {
      case nc: ConstraintDecorator => nc.inner // scalastyle:off
      case c: Constraint           => c // scalastyle:off
    }

    analysisBasedConstraint
      .asInstanceOf[AnalysisBasedConstraint[_, _, _]]
      .calculateAndEvaluate(df)
  }
}
