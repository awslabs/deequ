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
      assert(result.asInstanceOf[ComparisonFailed].errorMessage ==
        "The selected columns are not comparable due to duplicates present in the dataset." +
          "Comparison keys must be unique, but in Dataframe 1, there are 6 unique records and 7 rows, " +
          "and in Dataframe 2, there are 6 unique records and 6 rows, based on the combination of keys " +
          "{id} in Dataframe 1 and {id} in Dataframe 2")
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
      assert(result.left.get.errorMessage
        .contains("The selected columns are not comparable due to duplicates present in the dataset"))
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

  "Data Synchronization Schema Test for non key columns" should {
    def primaryDataset(spark: SparkSession, idColumnName: String): DataFrame = {
      import spark.implicits._
      spark.sparkContext.parallelize(
        Seq(
          (1, "John", "NY"),
          (2, "Javier", "WI"),
          (3, "Helena", "TX"),
          (4, "Helena", "TX"),
          (5, "Nick", "FL"),
          (6, "Molly", "TX")
        )
      ).toDF(idColumnName, "name", "state") // all lower case
    }

    def referenceDataset(spark: SparkSession, idColumnName: String): DataFrame = {
      import spark.implicits._
      spark.sparkContext.parallelize(
        Seq(
          (1, "John", "NY"),
          (2, "Javier", "WI"),
          (3, "Helena", "TX"),
          (4, "Helena", "TX"),
          (5, "Nicholas", "FL"),
          (6, "Ms Molly", "TX")
        )
      ).toDF(idColumnName, "Name", "State") // upper case except for id
    }

    "works when key column names have different casings" in withSparkSession { spark =>
      val id1ColumnName = "id"
      val id2ColumnName = "ID"
      val ds1 = primaryDataset(spark, id1ColumnName)
      val ds2 = referenceDataset(spark, id2ColumnName)

      // Not using id1ColumnName -> id2ColumnName intentionally.
      // In Glue DQ, we accept the column names in two formats: mapping and non-mapping
      // Mapping format is "col1 -> col2", when customer wants to compare columns with different names.
      // Non-mapping format is just "col1", when customer has same column, regardless of case, in both datasets.
      // A non-mapping format would translate it into the map below.
      // We want to test that the functionality works as expected in that case.
      val colKeyMap = Map(id1ColumnName -> id1ColumnName)

      // Overall
      val assertion: Double => Boolean = _ >= 0.6 // 4 out of 6 rows match
      val overallResult = DataSynchronization.columnMatch(ds1, ds2, colKeyMap, assertion)
      assert(overallResult.isInstanceOf[ComparisonSucceeded])

      // Row Level
      val outcomeColName = "outcome"
      val rowLevelResult = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, None, Some(outcomeColName))

      assert(rowLevelResult.isRight)

      val dfResult = rowLevelResult.right.get
      val rowLevelResults = dfResult.collect().map { row =>
        row.getAs[Int]("id") -> row.getAs[Boolean](outcomeColName)
      }.toMap

      val expected = Map(1 -> true, 2 -> true, 3 -> true, 4 -> true, 5 -> false, 6 -> false)
      assert(expected == rowLevelResults)
    }

    "works when non-key column names have different casings" in withSparkSession { spark =>
      val idColumnName = "id"
      val ds1 = primaryDataset(spark, idColumnName)
      val ds2 = referenceDataset(spark, idColumnName)

      val colKeyMap = Map(idColumnName -> idColumnName)

      // Overall
      val assertion: Double => Boolean = _ >= 0.6 // 4 out of 6 rows match
      val overallResult = DataSynchronization.columnMatch(ds1, ds2, colKeyMap, assertion)
      assert(overallResult.isInstanceOf[ComparisonSucceeded])

      // Row Level
      val outcomeColName = "outcome"
      val rowLevelResult = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, None, Some(outcomeColName))

      assert(rowLevelResult.isRight)

      val dfResult = rowLevelResult.right.get
      val rowLevelResults = dfResult.collect().map { row =>
        row.getAs[Int]("id") -> row.getAs[Boolean](outcomeColName)
      }.toMap

      val expected = Map(1 -> true, 2 -> true, 3 -> true, 4 -> true, 5 -> false, 6 -> false)
      assert(expected == rowLevelResults)
    }

    "fails as expected when key columns do not exist" in withSparkSession { spark =>
      val idColumnName = "id"
      val ds1 = primaryDataset(spark, idColumnName)
      val ds2 = referenceDataset(spark, idColumnName)
      val assertion: Double => Boolean = _ >= 0.6 // 4 out of 6 rows match

      val nonExistCol1 = "foo"
      val nonExistCol2 = "bar"

      // Key columns not in either dataset (Overall)
      val colKeyMap1 = Map(nonExistCol1 -> nonExistCol2)
      val overallResult1 = DataSynchronization.columnMatch(ds1, ds2, colKeyMap1, assertion)

      assert(overallResult1.isInstanceOf[ComparisonFailed])
      val failedOverallResult1 = overallResult1.asInstanceOf[ComparisonFailed]
      assert(failedOverallResult1.errorMessage.contains("key columns were not found in the first dataset"))
      assert(failedOverallResult1.errorMessage.contains(nonExistCol1))

      // Key columns not in either dataset (Row level)
      val rowLevelResult1 = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap1)
      assert(rowLevelResult1.isLeft)
      val failedRowLevelResult1 = rowLevelResult1.left.get
      assert(failedRowLevelResult1.errorMessage.contains("key columns were not found in the first dataset"))
      assert(failedRowLevelResult1.errorMessage.contains(nonExistCol1))

      // Key column not in first dataset
      val colKeyMap2 = Map(nonExistCol1 -> idColumnName)
      val overallResult2 = DataSynchronization.columnMatch(ds1, ds2, colKeyMap2, assertion)

      assert(overallResult2.isInstanceOf[ComparisonFailed])
      val failedOverallResult2 = overallResult2.asInstanceOf[ComparisonFailed]
      assert(failedOverallResult2.errorMessage.contains("key columns were not found in the first dataset"))
      assert(failedOverallResult2.errorMessage.contains(nonExistCol1))

      // Key column not in first dataset (Row level)
      val rowLevelResult2 = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap2)
      assert(rowLevelResult2.isLeft)
      val failedRowLevelResult2 = rowLevelResult2.left.get
      assert(failedRowLevelResult2.errorMessage.contains("key columns were not found in the first dataset"))
      assert(failedRowLevelResult2.errorMessage.contains(nonExistCol1))

      // Key column not in second dataset
      val colKeyMap3 = Map(idColumnName -> nonExistCol2)
      val overallResult3 = DataSynchronization.columnMatch(ds1, ds2, colKeyMap3, assertion)

      assert(overallResult3.isInstanceOf[ComparisonFailed])
      val failedOverallResult3 = overallResult3.asInstanceOf[ComparisonFailed]
      assert(failedOverallResult3.errorMessage.contains("key columns were not found in the second dataset"))
      assert(failedOverallResult3.errorMessage.contains(nonExistCol2))

      // Key column not in second dataset (Row level)
      val rowLevelResult3 = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap3)
      assert(rowLevelResult3.isLeft)
      val failedRowLevelResult3 = rowLevelResult3.left.get
      assert(failedRowLevelResult3.errorMessage.contains("key columns were not found in the second dataset"))
      assert(failedRowLevelResult3.errorMessage.contains(nonExistCol2))
    }

    "fails as expected when non-key columns do not exist" in withSparkSession { spark =>
      val idColumnName = "id"
      val ds1 = primaryDataset(spark, idColumnName)
      val ds2 = referenceDataset(spark, idColumnName)
      val assertion: Double => Boolean = _ >= 0.6 // 4 out of 6 rows match
      val colKeyMap = Map(idColumnName -> idColumnName)

      val nonExistCol1 = "foo"
      val nonExistCol2 = "bar"

      // Non-key columns not in either dataset (Overall)
      val compColsMap1 = Map(nonExistCol1 -> nonExistCol2)
      val overallResult1 = DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compColsMap1, assertion)

      assert(overallResult1.isInstanceOf[ComparisonFailed])
      val failedOverallResult1 = overallResult1.asInstanceOf[ComparisonFailed]
      assert(failedOverallResult1.errorMessage.contains(
        s"The following columns were not found in the first dataset: $nonExistCol1"))

      // Non-key columns not in either dataset (Row level)
      val rowLevelResult1 = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compColsMap1))
      assert(rowLevelResult1.isLeft)
      val failedRowLevelResult1 = rowLevelResult1.left.get
      assert(failedRowLevelResult1.errorMessage.contains(
        s"The following columns were not found in the first dataset: $nonExistCol1"))

      // Non-key column not in first dataset
      val compColsMap2 = Map(nonExistCol1 -> "State")
      val overallResult2 = DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compColsMap2, assertion)

      assert(overallResult2.isInstanceOf[ComparisonFailed])
      val failedOverallResult2 = overallResult2.asInstanceOf[ComparisonFailed]
      assert(failedOverallResult2.errorMessage.contains(
        s"The following columns were not found in the first dataset: $nonExistCol1"))

      // Non-key columns not in first dataset (Row level)
      val rowLevelResult2 = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compColsMap2))
      assert(rowLevelResult2.isLeft)
      val failedRowLevelResult2 = rowLevelResult2.left.get
      assert(failedRowLevelResult2.errorMessage.contains(
        s"The following columns were not found in the first dataset: $nonExistCol1"))

      // Non-key column not in second dataset
      val compColsMap3 = Map("state" -> nonExistCol2)
      val overallResult3 = DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compColsMap3, assertion)

      assert(overallResult3.isInstanceOf[ComparisonFailed])
      val failedOverallResult3 = overallResult3.asInstanceOf[ComparisonFailed]
      assert(failedOverallResult3.errorMessage.contains(
        s"The following columns were not found in the second dataset: $nonExistCol2"))

      // Non-key column not in second dataset (Row level)
      val rowLevelResult3 = DataSynchronization.columnMatchRowLevel(ds1, ds2, colKeyMap, Some(compColsMap3))
      assert(rowLevelResult3.isLeft)
      val failedRowLevelResult3 = rowLevelResult3.left.get
      assert(failedOverallResult3.errorMessage.contains(
        s"The following columns were not found in the second dataset: $nonExistCol2"))
    }
  }
}
