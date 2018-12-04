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
package checks

import com.amazon.deequ.analyzers.applicability.Applicability
import com.amazon.deequ.analyzers.{Completeness, Compliance, Maximum, Minimum}
import org.apache.spark.sql.types._
import org.scalatest.WordSpec

class ApplicabilityTest extends WordSpec with SparkContextSpec {

  private[this] val schema = StructType(Array(
      StructField("stringCol", StringType, nullable = true),
      StructField("stringCol2", StringType, nullable = true),
      StructField("byteCol", ByteType, nullable = true),
      StructField("shortCol", ShortType, nullable = true),
      StructField("intCol", IntegerType, nullable = true),
      StructField("intCol2", IntegerType, nullable = true),
      StructField("longCol", LongType, nullable = true),
      StructField("floatCol", FloatType, nullable = true),
      StructField("floatCol2", FloatType, nullable = true),
      StructField("doubleCol", DoubleType, nullable = true),
      StructField("doubleCol2", DoubleType, nullable = true),
      StructField("decimalCol", DecimalType.SYSTEM_DEFAULT, nullable = true),
      StructField("decimalCol2", DecimalType.SYSTEM_DEFAULT, nullable = true),
      StructField("decimalCol3", DecimalType(5, 2), nullable = true),
      StructField("decimalCol4", DecimalType(8, 4), nullable = true),
      StructField("timestampCol", TimestampType, nullable = true),
      StructField("timestampCol2", TimestampType, nullable = true),
      StructField("booleanCol", BooleanType, nullable = true),
      StructField("booleanCol2", BooleanType, nullable = true))
    )

  "Applicability tests for checks" should {

    "recognize applicable checks as applicable" in withSparkSession { session =>

      val applicability = new Applicability(session)

      val validCheck = Check(CheckLevel.Warning, "")
        .isComplete("stringCol")
        .isNonNegative("floatCol")

      val resultForValidCheck = applicability.isApplicable(validCheck, schema)

      assert(resultForValidCheck.isApplicable)
      assert(resultForValidCheck.failures.isEmpty)
      assert(resultForValidCheck.constraintApplicabilities.size == validCheck.constraints.size)
      resultForValidCheck.constraintApplicabilities.foreach { case (_, applicable) =>
        assert(applicable)
      }
    }

    "detect checks with non existing columns" in withSparkSession { session =>

      val applicability = new Applicability(session)

      val checkWithNonExistingColumn = Check(CheckLevel.Warning, "")
        .isComplete("stringColasd")

      val resultForCheckWithNonExistingColumn =
        applicability.isApplicable(checkWithNonExistingColumn, schema)

      assert(!resultForCheckWithNonExistingColumn.isApplicable)
      assert(resultForCheckWithNonExistingColumn.failures.size == 1)
      assert(resultForCheckWithNonExistingColumn.constraintApplicabilities.size ==
        checkWithNonExistingColumn.constraints.size)
      resultForCheckWithNonExistingColumn.constraintApplicabilities.foreach {
        case (_, applicable) => assert(!applicable)
      }
    }

    "detect checks with invalid sql expressions" in withSparkSession { session =>

      val applicability = new Applicability(session)

      val checkWithInvalidExpression1 = Check(CheckLevel.Warning, "")
        .isNonNegative("")

      val resultForCheckWithInvalidExpression1 =
        applicability.isApplicable(checkWithInvalidExpression1, schema)

      assert(!resultForCheckWithInvalidExpression1.isApplicable)
      assert(resultForCheckWithInvalidExpression1.failures.size == 1)


      val checkWithInvalidExpression2 = Check(CheckLevel.Warning, "")
        .isComplete("booleanCol").where("foo + bar___")

      val resultForCheckWithInvalidExpression2 =
        applicability.isApplicable(checkWithInvalidExpression2, schema)

      assert(!resultForCheckWithInvalidExpression2.isApplicable)
      assert(resultForCheckWithInvalidExpression2.failures.size == 1)
    }

    "report on all constraints of the Check" in withSparkSession { session =>
      val applicability = new Applicability(session)

      val check = Check(CheckLevel.Error, "")
        .isComplete("stringCol")
        .isUnique("stringCol")

      val result = applicability.isApplicable(check, schema)

      assert(result.constraintApplicabilities.size == check.constraints.size)

      check.constraints.foreach { constraint =>
        assert(result.constraintApplicabilities(constraint))
      }
    }
  }

  "Applicability tests for analyzers" should {

    "recognize applicable analyzers as applicable" in withSparkSession { session =>

      val applicability = new Applicability(session)

      val validAnalyzer = Completeness("stringCol")

      val resultForValidAnalyzer = applicability.isApplicable(Seq(validAnalyzer), schema)

      assert(resultForValidAnalyzer.isApplicable)
      assert(resultForValidAnalyzer.failures.isEmpty)
    }

    "detect analyzers for non existing columns" in withSparkSession { session =>

      val applicability = new Applicability(session)

      val analyzerForNonExistingColumn = Completeness("stringColasd")

      val resultForAnalyzerForNonExistingColumn =
        applicability.isApplicable(Seq(analyzerForNonExistingColumn), schema)

      assert(!resultForAnalyzerForNonExistingColumn.isApplicable)
      assert(resultForAnalyzerForNonExistingColumn.failures.size == 1)
    }

    "detect analyzers with invalid sql expressions" in withSparkSession { session =>

      val applicability = new Applicability(session)

      val analyzerWithInvalidExpression1 = Compliance("", "")

      val resultForAnalyzerWithInvalidExpression1 =
        applicability.isApplicable(Seq(analyzerWithInvalidExpression1), schema)

      assert(!resultForAnalyzerWithInvalidExpression1.isApplicable)
      assert(resultForAnalyzerWithInvalidExpression1.failures.size == 1)


      val analyzerWithInvalidExpression2 = Completeness("booleanCol", Some("foo + bar___"))

      val resultForAnalyzerWithInvalidExpression2 =
        applicability.isApplicable(Seq(analyzerWithInvalidExpression2), schema)

      assert(!resultForAnalyzerWithInvalidExpression2.isApplicable)
      assert(resultForAnalyzerWithInvalidExpression2.failures.size == 1)
    }

    "handles min/max with decimal columns" in withSparkSession { session =>

      val applicability = new Applicability(session)

      val analyzers = Seq(
        Minimum("decimalCol"),
        Maximum("decimalCol"),
        Minimum("decimalCol2"),
        Maximum("decimalCol2"),
        Minimum("decimalCol3"),
        Maximum("decimalCol3"),
        Minimum("decimalCol4"),
        Maximum("decimalCol4")
      )


      val resultForValidAnalyzer = applicability.isApplicable(analyzers, schema)

      assert(resultForValidAnalyzer.isApplicable)
      assert(resultForValidAnalyzer.failures.isEmpty)
    }

  }
}
