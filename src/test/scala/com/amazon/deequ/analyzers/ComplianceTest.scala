/**
 * Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ComplianceTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {
  "Compliance" should {
    "return row-level results for columns" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance("rule1", "att1 > 3", columns = List("att1"))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(_.getAs[Boolean]("new")
      ) shouldBe Seq(false, false, false, true, true, true)
    }

    "return row-level results for null columns" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance("rule1", "attNull > 3", columns = List("att1"))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(null, null, null, true, true, true)
    }

    "return row-level results filtered with null" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance("rule1", "att1 > 4", where = Option("att2 != 0"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(null, null, null, false, true, true)
    }

    "return row-level results filtered with true" in withSparkSession { session =>
      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance("rule1", "att1 > 4", where = Option("att2 != 0"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(true, true, true, false, true, true)
    }

    "return row-level results with NULL in where clause column treated as filtered" in withSparkSession { session =>
      import session.implicits._

      val data = Seq(
        ("1", "USA", "AUS"),  // matches WHERE, predicate passes -> true
        ("2", "GER", "AUS"),  // matches WHERE, predicate fails -> false
        ("3", "USA", null),   // NULL in WHERE col, should be filtered -> true
        ("4", "GER", null),   // NULL in WHERE col, should be filtered -> true
        ("5", "USA", "USA")   // doesn't match WHERE -> true (filtered)
      ).toDF("item", "championnationality", "runnerupnationality")

      val compliance = Compliance(
        "rule1",
        "championnationality IN ('USA', 'AUS')",
        where = Option("runnerupnationality = 'AUS'"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE))
      )

      val state = compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = compliance.computeMetricFrom(state)

      data.withColumn("result", metric.fullColumn.get).collect().map(r =>
        r.getAs[Boolean]("result")) shouldBe Seq(true, false, true, true, true)
    }

    "return row-level results for compliance in bounds" in withSparkSession { session =>
      val column = "att1"
      val leftOperand = ">="
      val rightOperand = "<="
      val lowerBound = 2
      val upperBound = 5
      val predicate = s"`$column` IS NULL OR " +
        s"(`$column` $leftOperand $lowerBound AND `$column` $rightOperand $upperBound)"

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance(predicate, s"$column between $lowerBound and $upperBound", columns = List("att3"))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(false, true, true, true, true, false)
    }

    "return row-level results for compliance in bounds filtered as null" in withSparkSession { session =>
      val column = "att1"
      val leftOperand = ">="
      val rightOperand = "<="
      val lowerBound = 2
      val upperBound = 5
      val predicate = s"`$column` IS NULL OR " +
        s"(`$column` $leftOperand $lowerBound AND `$column` $rightOperand $upperBound)"

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance(predicate, s"$column between $lowerBound and $upperBound",
        where = Option("att1 < 4"), columns = List("att3"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(false, true, true, null, null, null)
    }

    "return row-level results for compliance in bounds filtered as true" in withSparkSession { session =>
      val column = "att1"
      val leftOperand = ">="
      val rightOperand = "<="
      val lowerBound = 2
      val upperBound = 5
      val predicate = s"`$column` IS NULL OR " +
        s"(`$column` $leftOperand $lowerBound AND `$column` $rightOperand $upperBound)"

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance(s"$column between $lowerBound and $upperBound", predicate,
        where = Option("att1 < 4"), columns = List("att3"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(false, true, true, true, true, true)
    }

    "return row-level results for compliance in array" in withSparkSession { session =>
      val column = "att1"
      val allowedValues = Array("3", "4", "5")
      val valueList = allowedValues
        .map {
          _.replaceAll("'", "\\\\\'")
        }
        .mkString("'", "','", "'")

      val predicate = s"`$column` IS NULL OR `$column` IN ($valueList)"

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance(s"$column contained in ${allowedValues.mkString(",")}", predicate,
        columns = List("att3"))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(false, false, true, true, true, false)
    }

    "return row-level results for compliance in array filtered as null" in withSparkSession { session =>
      val column = "att1"
      val allowedValues = Array("3", "4", "5")
      val valueList = allowedValues
        .map {
          _.replaceAll("'", "\\\\\'")
        }
        .mkString("'", "','", "'")

      val predicate = s"`$column` IS NULL OR `$column` IN ($valueList)"

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance(s"$column contained in ${allowedValues.mkString(",")}", predicate,
        where = Option("att1 < 5"), columns = List("att3"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(false, false, true, true, null, null)
    }

    "return row-level results for compliance in array filtered as true" in withSparkSession { session =>
      val column = "att1"
      val allowedValues = Array("3", "4", "5")
      val valueList = allowedValues
        .map {
          _.replaceAll("'", "\\\\\'")
        }
        .mkString("'", "','", "'")

      val predicate = s"`$column` IS NULL OR `$column` IN ($valueList)"

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance(s"$column contained in ${allowedValues.mkString(",")}", predicate,
        where = Option("att1 < 5"), columns = List("att3"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Boolean]("new")) shouldBe Seq(false, false, true, true, true, true)
    }
  }
}
