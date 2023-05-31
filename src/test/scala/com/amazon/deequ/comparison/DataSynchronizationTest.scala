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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

private [comparison] case class Data(name: String, state: String)
private [comparison] case class DataRow(id: Int, data: Data)

class DataSynchronizationTest extends AnyWordSpec with SparkContextSpec {

  "Data Synchronization Test" should {
    "match == 0.66 when id is colKey and name is compCols" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("id" -> "id")
      val compCols = Map("name" -> "name")
      val assertion: Double => Boolean = _ >= 0.60

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "match == 0.83 when id is colKey and state is compCols" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("id" -> "id")
      val compCols = Map("state" -> "state")
      val assertion: Double => Boolean = _ >= 0.80

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "return false because col name isn't unique" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("name" -> "name")
      val compCols = Map("state" -> "state")
      val assertion: Double => Boolean = _ >= 0.66

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "match >= 0.66 when id is unique col, rest compCols" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("id" -> "id")
      val compCols = Map("name" -> "name", "state" -> "state")
      val assertion: Double => Boolean = _ >= 0.60

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "match >= 0.66 (same test as above only the data sets change)" in withSparkSession{ spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS2
      val ds2 = testDS1
      val colKeyMap = Map("id" -> "id")
      val compCols = Map("name" -> "name", "state" -> "state")
      val assertion: Double => Boolean = _ >= 0.60

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "return false because the id col in ds1 isn't unique" in withSparkSession { spark =>
      import spark.implicits._

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val rdd3 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (1, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (9, "Nicholas", "CT"),
        (7, "Megan", "TX")))
      val testDS3 = rdd3.toDF("id", "name", "state")

      val ds1 = testDS3
      val ds2 = testDS2
      val colKeyMap = Map("id" -> "id")
      val compCols = Map("name" -> "name", "state" -> "state")
      val assertion: Double => Boolean = _ >= 0.40

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.asInstanceOf[ComparisonFailed].errorMessage == "The selected dataset columns are not comparable " +
        "using the specified keys. " +
        s"Dataframe 1 has 6 unique combinations and 7 rows," +
        s" and " +
        s"Dataframe 2 has 6 unique combinations and 6 rows.")
    }

    "return false because the id col in ds2 isn't unique" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd3 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (1, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (9, "Nicholas", "CT"),
        (7, "Megan", "TX")))
      val testDS3 = rdd3.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS3
      val colKeyMap = Map("id" -> "id")
      val compCols = Map("name" -> "name", "state" -> "state")
      val assertion: Double => Boolean = _ >= 0.40

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "return false because col state isn't unique" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("state" -> "state")
      val compCols = Map("name" -> "name")
      val assertion: Double => Boolean = _ >= 0.66

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "check all columns and return an assertion of .66" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("id" -> "id")
      val assertion: Double => Boolean = _ >= 0.66

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, assertion))
      assert(result.isInstanceOf[ComparisonSucceeded])
    }

    "return false because state column isn't unique" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("state" -> "state")
      val assertion: Double => Boolean = _ >= 0.66

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, assertion))
      assert(result.isInstanceOf[ComparisonFailed])
    }

    "check all columns" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("id" -> "id")
      val assertion: Double => Boolean = _ >= 0.66

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, assertion))
      assert(result.isInstanceOf[ComparisonSucceeded])
    }
    "cols exist but 0 matches" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX")))
      val testDS1 = rdd1.toDF("id", "name", "state")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (7, "Rahul", "CA"),
        (8, "Tom", "MX"),
        (9, "Tome", "NM"),
        (10, "Kyra", "AZ"),
        (11, "Jamie", "NB"),
        (12, "Andrius", "ID")))
      val testDS2 = rdd2.toDF("id", "name", "state")

      val ds1 = testDS1
      val ds2 = testDS2
      val colKeyMap = Map("id" -> "id")
      val assertion: Double => Boolean = _ >= 0

      val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, assertion))
      assert(result.isInstanceOf[ComparisonSucceeded])
    }
  }

  "Data Synchronization Row Level Test" should {
    def primaryDataset(spark: SparkSession): DataFrame = {
      import spark.implicits._
      spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "TX"),
        (5, "Nick", "FL"),
        (6, "Molly", "TX"))
      ).toDF("id", "name", "state")
    }

    def referenceDataset(spark: SparkSession): DataFrame = {
      import spark.implicits._
      spark.sparkContext.parallelize(Seq(
        (1, "John", "NY"),
        (2, "Javier", "WI"),
        (3, "Helena", "TX"),
        (4, "Helena", "WA"),
        (5, "Helena", "FL"),
        (7, "Megan", "TX"))
      ).toDF("id2", "name2", "state2")
    }

    "annotate primary data frame with row level results for name" in withSparkSession { spark =>
      val ds1 = primaryDataset(spark)
      val ds2 = referenceDataset(spark)

      val colKeyMap = Map("id" -> "id2")
      val compCols = Map("name" -> "name2")

      val outcomeColName = "outcome"
      val result = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compCols), Some(outcomeColName))
      assert(result.isRight)

      val dfResult = result.right.get

      // All columns from testDS1 should be in final output
      assert(compCols.keys.forall(c => Try { dfResult(c) }.isSuccess))
      // Any columns from testDS2 should not be in final output
      assert(compCols.values.forall(c => Try { dfResult(c) }.isFailure))

      val rowLevelResults = dfResult.collect().map { row =>
        row.getAs[Int]("id") -> row.getAs[Boolean](outcomeColName)
      }.toMap

      val expected = Map(1 -> true, 2 -> true, 3 -> true, 4 -> true, 5 -> false, 6 -> false)
      assert(expected == rowLevelResults)
    }

    "annotate primary data frame with row level results for name and state column" in withSparkSession { spark =>
      val ds1 = primaryDataset(spark)
      val ds2 = referenceDataset(spark)

      val colKeyMap = Map("id" -> "id2")
      val compCols = Map("name" -> "name2", "state" -> "state2")

      val outcomeColName = "outcome"
      val result = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compCols), Some(outcomeColName))
      assert(result.isRight)

      val dfResult = result.right.get

      // All columns from testDS1 should be in final output
      assert(compCols.keys.forall(c => Try { dfResult(c) }.isSuccess))
      // Any columns from testDS2 should not be in final output
      assert(compCols.values.forall(c => Try { dfResult(c) }.isFailure))

      val rowLevelResults = dfResult.collect().map { row =>
        row.getAs[Int]("id") -> row.getAs[Boolean](outcomeColName)
      }.toMap

      val expected = Map(1 -> true, 2 -> true, 3 -> true, 4 -> false, 5 -> false, 6 -> false)
      assert(expected == rowLevelResults)
    }

    "annotate primary data frame with row level results for " +
      "name and state column with additional id columns" in withSparkSession { spark =>
      val ds1 = primaryDataset(spark)
      val ds2 = referenceDataset(spark)

      val colKeyMap = Map("id" -> "id2", "name" -> "name2") // id is enough as primary key but we provided name as well
      val compCols = Map("name" -> "name2", "state" -> "state2")

      val outcomeColName = "outcome"
      val result = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compCols), Some(outcomeColName))
      assert(result.isRight)

      val dfResult = result.right.get

      // All columns from testDS1 should be in final output
      assert(compCols.keys.forall(c => Try { dfResult(c) }.isSuccess))
      // Any columns from testDS2 should not be in final output
      assert(compCols.values.forall(c => Try { dfResult(c) }.isFailure))

      val rowLevelResults = dfResult.collect().map { row =>
        row.getAs[Int]("id") -> row.getAs[Boolean](outcomeColName)
      }.toMap

      val expected = Map(1 -> true, 2 -> true, 3 -> true, 4 -> false, 5 -> false, 6 -> false)
      assert(expected == rowLevelResults)
    }

    "fails to annotate row level results when incorrect id column provided" in withSparkSession { spark =>
      val ds1 = primaryDataset(spark)
      val ds2 = referenceDataset(spark)

      val compCols = Map("name" -> "name2")
      val colKeyMap = compCols // Should use id, but used name instead

      val outcomeColName = "outcome"
      val result = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compCols), Some(outcomeColName))
      assert(result.isLeft)
      assert(result.left.get.errorMessage.contains("The selected dataset columns are not comparable"))
    }

    "fails to annotate row level results when column key map is not provided " +
      "and non key columns do not match" in withSparkSession { spark =>
      val ds1 = primaryDataset(spark)
      val ds2 = referenceDataset(spark)

      val colKeyMap = Map("id" -> "id2")
      val compColsMap: Option[Map[String, String]] = None

      val outcomeColName = "outcome"

      val result = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, compColsMap, Some(outcomeColName))
      assert(result.isLeft)
      assert(result.left.get.errorMessage.contains("Non key columns in the given data frames do not match"))
    }

    "fails to annotate row level results when empty column comparison map provided" in withSparkSession { spark =>
      val ds1 = primaryDataset(spark)
      val ds2 = referenceDataset(spark)

      val colKeyMap = Map("id" -> "id2")
      val compColsMap: Option[Map[String, String]] = Some(Map.empty)

      val outcomeColName = "outcome"

      val result = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, compColsMap, Some(outcomeColName))
      assert(result.isLeft)
      assert(result.left.get.errorMessage.contains("Empty column comparison map provided"))
    }
  }

  "Data Synchronization Row Level Test" should {
    def primaryDataset(spark: SparkSession): DataFrame = {
      import spark.implicits._
      spark.sparkContext.parallelize(Seq(
        DataRow(1, Data("John", "NY")),
        DataRow(2, Data("Javier", "WI")),
        DataRow(3, Data("Helena", "TX")),
        DataRow(4, Data("Helena", "TX")),
        DataRow(5, Data("Nick", "FL")),
        DataRow(6, Data("Molly", "TX")))
      ).toDF()
    }

    def referenceDataset(spark: SparkSession): DataFrame = {
      import spark.implicits._
      spark.sparkContext.parallelize(Seq(
        DataRow(1, Data("John", "NY")),
        DataRow(2, Data("Javier", "WI")),
        DataRow(3, Data("Helena", "TX")),
        DataRow(4, Data("Helena", "WA")),
        DataRow(5, Data("Helena", "FL")),
        DataRow(7, Data("Megan", "TX")))
      ).toDF()
    }

    "annotate primary data frame with row level results for struct type data column" in withSparkSession { spark =>
      val ds1 = primaryDataset(spark)
      val ds2 = referenceDataset(spark)

      val colKeyMap = Map("id" -> "id")
      val compCols = Map("data" -> "data")

      val outcomeColName = "outcome"
      val result = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compCols), Some(outcomeColName))
      assert(result.isRight)

      val dfResult = result.right.get
      val rowLevelResults = dfResult.collect().map { row =>
        row.getAs[Int]("id") -> row.getAs[Boolean](outcomeColName)
      }.toMap

      val expected = Map(1 -> true, 2 -> true, 3 -> true, 4 -> false, 5 -> false, 6 -> false)
      assert(expected == rowLevelResults)
    }
  }
}
