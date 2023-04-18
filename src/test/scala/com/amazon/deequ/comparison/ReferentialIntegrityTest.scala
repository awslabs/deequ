/**
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.comparison

import com.amazon.deequ.SparkContextSpec
import org.scalatest.wordspec.AnyWordSpec

private[comparison] case class State(stateName: String, stateAbbr: String)
private[comparison] case class DatasetRow(id: Int, state: State)

private[comparison] case class StateReverse(stateAbbr: String, stateName: String)
private[comparison] case class DatasetRowReverse(id: Int, state: StateReverse)

class ReferentialIntegrityTest extends AnyWordSpec with SparkContextSpec {
  "Referential Integrity Test" should {
    "primary columns being empty throws error" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col = "name"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq.empty, ds2, Seq(col), assertion)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].errorMessage.contains(s"Empty list provided"))
    }

    "primary column not being in primary throws error" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "name_foo"
      val col2 = "name"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].errorMessage.contains(s"does not exist in primary data frame"))
    }

    "primary columns not being in primary throws error" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val cols = Seq("name_foo", "state_foo")
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, cols, ds2, cols, assertion)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].errorMessage.contains(s"do not exist in primary data frame"))
    }

    "reference columns being empty throws error" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col = "name"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col), ds2, Seq.empty, assertion)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].errorMessage.contains(s"Empty list provided"))
    }

    "reference column not being in reference throws error" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "name"
      val col2 = "name_foo"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].errorMessage.contains(s"does not exist in reference data frame"))
    }

    "reference columns not being in primary throws error" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val cols1 = Seq("name", "state")
      val cols2 = Seq("name_foo", "state_foo")
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, cols1, ds2, cols2, assertion)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].errorMessage.contains(s"do not exist in reference data frame"))
    }

    "primary and reference columns size being different throws error" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "name_foo"
      val col2 = "name"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1, col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonFailed])
      assert(result.asInstanceOf[ComparisonFailed].errorMessage.contains(s"must equal"))
    }

    "id match equals 1.0" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "id"
      val col2 = "new_id"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "id match equals 0.60" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "new_id"
      val col2 = "id"
      val assertion: Double => Boolean = _ == 0.6

      val result = ReferentialIntegrity.subsetCheck(ds2, Seq(col1), ds1, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "name match equals 1.0" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "name"
      val col2 = "name"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "name match equals 0.60" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("id", "name", "state")

      val col1 = "name"
      val col2 = "name"
      val assertion: Double => Boolean = _ == 0.6

      val result = ReferentialIntegrity.subsetCheck(ds2, Seq(col1), ds1, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "state match equals 1.0" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "state"
      val col2 = "state"
      val assertion: Double => Boolean = _ >= 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "state match equals to 0.80" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "state"
      val col2 = "state"
      val assertion: Double => Boolean = _ >= 0.8

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "state match with name equals to 0.0" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "name"
      val col2 = "state"
      val assertion: Double => Boolean = _ >= 0.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "ds1 doesn't contain col1 " in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "ids"
      val col2 = "new_id"
      val assertion: Double => Boolean = _ == 0.66

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "ds2 doesn't contain col2" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "id"
      val col2 = "all-ids"
      val assertion: Double => Boolean = _ == 0.66

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "More rows still 1.0" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (3, "Helena", "TX"),
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (5, "Tyler", "FL"),
        (6, "Megan", "TX")))
      val ds2 = rdd2.toDF("new_id", "name", "state")

      val col1 = "id"
      val col2 = "new_id"
      val assertion: Double => Boolean = _ == 1.0

      val result = ReferentialIntegrity.subsetCheck(ds1, Seq(col1), ds2, Seq(col2), assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "Multiple columns" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "Canada", "CA"))) // Incorrect row
      val ds1 = rdd1.toDF("id", "state name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "California", "CA"))) // Reference has correct row
      val ds2 = rdd2.toDF("id", "state name", "state")

      val cols = Seq("state name", "state")
      val assertion: Double => Boolean = _ == 0.75

      val result = ReferentialIntegrity.subsetCheck(ds1, cols, ds2, cols, assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "Multiple non-nested columns with dots in the column names" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "Canada", "CA"))) // Incorrect row
      val ds1 = rdd1.toDF("id", "state.name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "California", "CA"))) // Reference has correct row
      val ds2 = rdd2.toDF("id", "state.name", "state")

      val cols = Seq("`state.name`", "state")
      val assertion: Double => Boolean = _ == 0.75

      val result = ReferentialIntegrity.subsetCheck(ds1, cols, ds2, cols, assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "Multiple nested columns" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(
        Seq(
          DatasetRow(1, State("New York", "NY")),
          DatasetRow(1, State("Wisconsin", "WI")),
          DatasetRow(1, State("Texas", "TX")),
          DatasetRow(1, State("Canada", "CA")) // Incorrect row
        )
      )
      val ds1 = rdd1.toDF

      val rdd2 = spark.sparkContext.parallelize(
        Seq(
          DatasetRow(1, State("New York", "NY")),
          DatasetRow(1, State("Wisconsin", "WI")),
          DatasetRow(1, State("Texas", "TX")),
          DatasetRow(1, State("California", "CA")) // Reference has correct row
        )
      )
      val ds2 = rdd2.toDF

      val cols = Seq("state.stateName", "state.stateAbbr")
      val assertion: Double => Boolean = _ == 0.75

      val result = ReferentialIntegrity.subsetCheck(ds1, cols, ds2, cols, assertion)
      assert(result.isInstanceOf[ComparisonSucceeded])
    }
  }

  "Referential Integrity Row Level Test" should {
    "works for multiple columns" in withSparkSession { spark =>
      import spark.implicits._

      val idColumn = "id"
      val refColumns = Seq("state name", "state")
      val allColumns = idColumn +: refColumns

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "Canada", "CA"))) // Incorrect row
      val ds1 = rdd1.toDF(allColumns: _*)

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "California", "CA"))) // Reference has correct row
      val ds2 = rdd2.toDF(allColumns: _*)

      val outcomeCol = "row_level_outcome"

      val result = ReferentialIntegrity.subsetCheckRowLevel(ds1, refColumns, ds2, refColumns, Some(outcomeCol))
      assert(result.isRight)

      val resultDF = result.right.get
      assert(resultDF.columns.toSeq == allColumns :+ outcomeCol)

      val outcomes = resultDF.orderBy(idColumn).select(outcomeCol).collect().toSeq.map { r => r.get(0) }
      assert(outcomes == Seq(true, true, true, false))

      assert(ds1.orderBy(idColumn).collect() sameElements resultDF.orderBy(idColumn).drop(outcomeCol).collect())
    }

    "works for multiple columns with duplicates in reference" in withSparkSession { spark =>
      import spark.implicits._

      val idColumn = "id"
      val refColumns = Seq("state name", "state")
      val allColumns = idColumn +: refColumns

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "Canada", "CA"))) // Incorrect row
      val ds1 = rdd1.toDF(allColumns: _*)

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "New York", "NY"),
        (2, "Wisconsin", "WI"),
        (3, "Texas", "TX"),
        (4, "California", "CA"), // Reference has correct row
        (5, "California", "CA"))) // Reference has correct duplicate row
      val ds2 = rdd2.toDF(allColumns: _*)

      val outcomeCol = "row_level_outcome"

      val result = ReferentialIntegrity.subsetCheckRowLevel(ds1, refColumns, ds2, refColumns, Some(outcomeCol))
      assert(result.isRight)

      val resultDF = result.right.get
      assert(resultDF.columns.toSeq == allColumns :+ outcomeCol)

      val outcomes = resultDF.orderBy(idColumn).select(outcomeCol).collect().toSeq.map { r => r.get(0) }
      assert(outcomes == Seq(true, true, true, false))

      assert(ds1.orderBy(idColumn).collect() sameElements resultDF.orderBy(idColumn).drop(outcomeCol).collect())
    }

    "works for nested columns" in withSparkSession { spark =>
      import spark.implicits._

      val idColumn = "id"
      val refColumns = Seq("state.stateName", "state.stateAbbr")
      val allColumns = Seq(idColumn, "state") // State is a single, nested column

      val rdd1 = spark.sparkContext.parallelize(
        Seq(
          DatasetRow(1, State("New York", "NY")),
          DatasetRow(2, State("Wisconsin", "WI")),
          DatasetRow(3, State("Texas", "TX")),
          DatasetRow(4, State("Canada", "CA")) // Incorrect row
        )
      )
      val ds1 = rdd1.toDF

      val rdd2 = spark.sparkContext.parallelize(
        Seq(
          DatasetRow(1, State("New York", "NY")),
          DatasetRow(2, State("Wisconsin", "WI")),
          DatasetRow(3, State("Texas", "TX")),
          DatasetRow(4, State("California", "CA")) // Reference has correct row
        )
      )
      val ds2 = rdd2.toDF

      val outcomeCol = "row_level_outcome"

      val result = ReferentialIntegrity.subsetCheckRowLevel(ds1, refColumns, ds2, refColumns, Some(outcomeCol))
      assert(result.isRight)

      val resultDF = result.right.get
      assert(resultDF.columns.toSeq == allColumns :+ outcomeCol)

      val outcomes = resultDF.orderBy(idColumn).select(outcomeCol).collect().toSeq.map { r => r.get(0) }
      assert(outcomes == Seq(true, true, true, false))

      assert(ds1.orderBy(idColumn).collect() sameElements resultDF.orderBy(idColumn).drop(outcomeCol).collect())
    }

    "works for nested columns with reference columns in reverse order" in withSparkSession { spark =>
      import spark.implicits._

      val idColumn = "id"
      val refColumns = Seq("state.stateName", "state.stateAbbr")
      val allColumns = Seq(idColumn, "state") // State is a single, nested column

      val rdd1 = spark.sparkContext.parallelize(
        Seq(
          DatasetRow(1, State("New York", "NY")),
          DatasetRow(2, State("Wisconsin", "WI")),
          DatasetRow(3, State("Texas", "TX")),
          DatasetRow(4, State("Canada", "CA")) // Incorrect row
        )
      )
      val ds1 = rdd1.toDF

      val rdd2 = spark.sparkContext.parallelize(
        Seq(
          DatasetRowReverse(1, StateReverse("NY", "New York")),
          DatasetRowReverse(2, StateReverse("WI", "Wisconsin")),
          DatasetRowReverse(3, StateReverse("TX", "Texas")),
          DatasetRowReverse(4, StateReverse("CA", "California")) // Reference has correct row
        )
      )
      val ds2 = rdd2.toDF

      val outcomeCol = "row_level_outcome"

      val result = ReferentialIntegrity.subsetCheckRowLevel(ds1, refColumns, ds2, refColumns, Some(outcomeCol))
      assert(result.isRight)

      val resultDF = result.right.get
      assert(resultDF.columns.toSeq == allColumns :+ outcomeCol)

      val outcomes = resultDF.orderBy(idColumn).select(outcomeCol).collect().toSeq.map { r => r.get(0) }
      assert(outcomes == Seq(true, true, true, false))

      assert(ds1.orderBy(idColumn).collect() sameElements resultDF.orderBy(idColumn).drop(outcomeCol).collect())
    }
  }
}
