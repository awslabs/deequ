/**
 * Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package analyzers

import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Success

class DuplicateRowCountTest extends AnyWordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "DuplicateRowCount" should {

    "count duplicate rows correctly" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1), ("b", 2), ("a", 1), ("c", 3), ("a", 1)
      ).toDF("col1", "col2")
      // ("a", 1) appears 3 times -> 3 duplicate rows
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe Success(3.0)
    }

    "return 0 when no duplicates exist" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1), ("b", 2), ("c", 3)
      ).toDF("col1", "col2")
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe Success(0.0)
    }

    "return total row count when all rows are identical" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1), ("a", 1), ("a", 1)
      ).toDF("col1", "col2")
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe Success(3.0)
    }

    "return 0 for a single row" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(("a", 1)).toDF("col1", "col2")
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe Success(0.0)
    }

    "handle multiple duplicate groups" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1), ("b", 2), ("a", 1), ("b", 2), ("c", 3)
      ).toDF("col1", "col2")
      // ("a",1) x2 + ("b",2) x2 = 4 duplicate rows
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe Success(4.0)
    }

    "treat NULLs as equal for grouping" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        (Some("a"), Some(1)),
        (Some("a"), None),
        (Some("a"), None)
      ).toDF("col1", "col2")
      // ("a", null) appears twice -> 2 duplicate rows
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe Success(2.0)
    }

    "exclude rows where all grouping columns are NULL" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        (None: Option[String], None: Option[Int]),
        (None: Option[String], None: Option[Int]),
        (Some("a"), Some(1))
      ).toDF("col1", "col2")
      // All-null rows are excluded, ("a",1) appears once -> 0 duplicates
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe Success(0.0)
    }

    "work with subset of columns" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1, "x"), ("a", 2, "y"), ("b", 3, "z")
      ).toDF("col1", "col2", "col3")
      // Checking only col1: "a" appears twice -> 2 duplicate rows
      val result = DuplicateRowCount(Seq("col1")).calculate(df)
      result.value shouldBe Success(2.0)
    }

    "apply where clause correctly" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1, "active"), ("a", 1, "inactive"), ("a", 1, "active"), ("b", 2, "active")
      ).toDF("col1", "col2", "status")
      // With where "status = 'active'": ("a", 1, "active") appears twice -> 2 duplicates
      val result = DuplicateRowCount(Seq("col1", "col2", "status"),
        where = Some("status = 'active'")).calculate(df)
      result.value shouldBe Success(2.0)
    }

    "return 0 when where clause matches no rows" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1, "active"), ("a", 1, "active"), ("b", 2, "active")
      ).toDF("col1", "col2", "status")
      val result = DuplicateRowCount(Seq("col1", "col2", "status"),
        where = Some("status = 'nonexistent'")).calculate(df)
      result.value shouldBe Success(0.0)
    }

    "use all columns when columns list is empty" in withSparkSession { session =>
      import session.implicits._
      val df = Seq(
        ("a", 1), ("b", 2), ("a", 1), ("c", 3)
      ).toDF("col1", "col2")
      // ("a", 1) appears twice -> 2 duplicate rows
      val result = DuplicateRowCount(Seq.empty).calculate(df)
      result.value shouldBe Success(2.0)
    }

    "return 0 for empty DataFrame" in withSparkSession { session =>
      import session.implicits._
      val df = session.createDataFrame(
        session.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        org.apache.spark.sql.types.StructType(Seq(
          org.apache.spark.sql.types.StructField("col1", org.apache.spark.sql.types.StringType),
          org.apache.spark.sql.types.StructField("col2", org.apache.spark.sql.types.IntegerType)
        ))
      )
      val result = DuplicateRowCount(Seq("col1", "col2")).calculate(df)
      result.value shouldBe scala.util.Success(0.0)
    }
  }
}
