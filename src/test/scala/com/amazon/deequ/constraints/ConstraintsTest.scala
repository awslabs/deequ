/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ
package constraints

import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}
import ConstraintUtils.calculate
import com.amazon.deequ.analyzers.{Completeness, NumMatchesAndCount}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType}
import Constraint._
import com.amazon.deequ.SparkContextSpec

class ConstraintsTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Completeness constraint" should {
    "assert on wrong completeness" in withSparkSession { sparkSession =>
      val df = getDfMissing(sparkSession)
      assert(calculate(Constraint.completenessConstraint("att1", _ == 0.5), df).status ==
        ConstraintStatus.Success)
      assert(calculate(Constraint.completenessConstraint("att1", _ != 0.5), df).status ==
        ConstraintStatus.Failure)
      assert(calculate(Constraint.completenessConstraint("att2", _ == 0.75), df).status ==
        ConstraintStatus.Success)
      assert(calculate(Constraint.completenessConstraint("att2", _ != 0.75), df).status ==
        ConstraintStatus.Failure)
    }
  }

  "Histogram constraints" should {
    "assert on bin number" in withSparkSession { sparkSession =>
      val df = getDfMissing(sparkSession)
      assert(calculate(Constraint.histogramBinConstraint("att1", _ == 3), df).status ==
        ConstraintStatus.Success)
      assert(calculate(Constraint.histogramBinConstraint("att1", _ != 3), df).status ==
        ConstraintStatus.Failure)
    }
    "assert on ratios for a column value which does not exist" in withSparkSession { sparkSession =>
      val df = getDfMissing(sparkSession)

      val metric = calculate(Constraint.histogramConstraint("att1",
        _("non-existent-column-value").ratio == 3), df)

      metric match {
        case result =>
          assert(result.status == ConstraintStatus.Failure)
          assert(result.message.isDefined)
          assert(result.message.get.startsWith(AnalysisBasedConstraint.AssertionException))
      }
    }
  }

  "Mutual information constraint" should {
    "yield a mutual information of 0 for conditionally uninformative columns" in
      withSparkSession { sparkSession =>
        val df = getDfWithConditionallyUninformativeColumns(sparkSession)
        calculate(Constraint.mutualInformationConstraint("att1", "att2", _ == 0), df)
          .status shouldBe ConstraintStatus.Success
      }
  }

  "Basic stats constraints" should {
    "assert on approximate quantile" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      calculate(Constraint.approxQuantileConstraint("att1", quantile = 0.5, _ == 3.0), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert on minimum" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      calculate(Constraint.minConstraint("att1", _ == 1.0), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert on maximum" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      calculate(Constraint.maxConstraint("att1", _ == 6.0), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert on mean" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      calculate(Constraint.meanConstraint("att1", _ == 3.5), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert on sum" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      calculate(Constraint.sumConstraint("att1", _ == 21), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert on standard deviation" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      calculate(Constraint.standardDeviationConstraint("att1", _ == 1.707825127659933), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert on approximate count distinct" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      calculate(Constraint.approxCountDistinctConstraint("att1", _ == 6.0), df)
        .status shouldBe ConstraintStatus.Success
    }
  }

  "Min and max string length stats constraints" should {
    "assert on min length" in withSparkSession { sparkSession =>
      val df = getDfWithVariableStringLengthValues(sparkSession)
      calculate(Constraint.minLengthConstraint("att1", _ == 0.0), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert on max length" in withSparkSession { sparkSession =>
      val df = getDfWithVariableStringLengthValues(sparkSession)
      calculate(Constraint.maxLengthConstraint("att1", _ == 4.0), df)
        .status shouldBe ConstraintStatus.Success
    }
  }

  "Correlation constraint" should {
    "assert maximal correlation" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      calculate(Constraint.correlationConstraint("att1", "att2", _ == 1.0), df)
        .status shouldBe ConstraintStatus.Success
    }
    "assert no correlation" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyUninformativeColumns(sparkSession)
      calculate(Constraint.correlationConstraint("att1", "att2", java.lang.Double.isNaN), df)
        .status shouldBe ConstraintStatus.Success
    }
  }

  "Data type constraint" should {
    val column = "column"

    "assert fractional type for DoubleType column" in withSparkSession { sparkSession =>
      val df = dataFrameWithColumn(column, DoubleType, sparkSession, Row(1.0), Row(2.0))
      calculate(dataTypeConstraint(column, ConstrainableDataTypes.Fractional, _ == 1.0), df)
        .status shouldBe ConstraintStatus.Success
    }

    "assert fractional type for StringType column" in withSparkSession { sparkSession =>
      val df = dataFrameWithColumn(column, StringType, sparkSession, Row("1"), Row("2.0"))
      calculate(dataTypeConstraint(column, ConstrainableDataTypes.Fractional, _ == 0.5), df)
        .status shouldBe ConstraintStatus.Success
    }

    "assert numeric type as sum over fractional and integral" in withSparkSession { sparkSession =>
      val df = dataFrameWithColumn(column, StringType, sparkSession, Row("1"), Row("2.0"))
      calculate(dataTypeConstraint(column, ConstrainableDataTypes.Numeric, _ == 1.0), df)
        .status shouldBe ConstraintStatus.Success
    }
  }

  "Anomaly constraint" should {
    "assert on anomaly analyzer values" in withSparkSession { sparkSession =>
      val df = getDfMissing(sparkSession)
      assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
        Completeness("att1"), _ > 0.4), df).status == ConstraintStatus.Success)
      assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
        Completeness("att1"), _ < 0.4), df).status == ConstraintStatus.Failure)

      assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
        Completeness("att2"), _ > 0.7), df).status == ConstraintStatus.Success)
      assert(calculate(Constraint.anomalyConstraint[NumMatchesAndCount](
        Completeness("att2"), _ < 0.7), df).status == ConstraintStatus.Failure)
    }
  }
}
