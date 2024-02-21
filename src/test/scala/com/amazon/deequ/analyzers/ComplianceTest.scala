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

      data.withColumn("new", metric.fullColumn.get).collect().map(_.getAs[Int]("new")
      ) shouldBe Seq(0, 0, 0, 1, 1, 1)
    }

    "return row-level results for null columns" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance("rule1", "attNull > 3", columns = List("att1"))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(null, null, null, 1, 1, 1)
    }

    "return row-level results filtered with null" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance("rule1", "att1 > 4", where = Option("att2 != 0"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.NULL)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(null, null, null, 0, 1, 1)
    }

    "return row-level results filtered with true" in withSparkSession { session =>

      val data = getDfWithNumericValues(session)

      val att1Compliance = Compliance("rule1", "att1 > 4", where = Option("att2 != 0"),
        analyzerOptions = Option(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)))
      val state = att1Compliance.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = att1Compliance.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).collect().map(r =>
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(1, 1, 1, 0, 1, 1)
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
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(0, 1, 1, 1, 1, 0)
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
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(0, 1, 1, null, null, null)
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
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(0, 1, 1, 1, 1, 1)
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
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(0, 0, 1, 1, 1, 0)
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
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(0, 0, 1, 1, null, null)
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
        if (r == null) null else r.getAs[Int]("new")) shouldBe Seq(0, 0, 1, 1, 1, 1)
    }
  }

}
