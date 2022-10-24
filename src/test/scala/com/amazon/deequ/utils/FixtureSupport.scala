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

package com.amazon.deequ.utils

import org.apache.spark.sql.types.{
  DoubleType,
  LongType,
  MapType,
  StringType,
  StructType
}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.amazon.deequ.profiles.{ColumnProfile, StandardColumnProfile}
import com.amazon.deequ.analyzers.{DataTypeInstances}
import com.amazon.deequ.metrics.Distribution
import org.scalamock.scalatest.MockFactory

import scala.util.Random

trait FixtureSupport extends MockFactory {

  def getEmptyColumnDataDf(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("", "a", "f"),
      ("", "b", "d"),
      ("", "a", null),
      ("", "a", "f"),
      ("", "b", null),
      ("", "a", "f")
    ).toDF("att1", "att2", "att3")
  }

  def getDfEmpty(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val column1 = $"column1".string
    val column2 = $"column2".string
    val mySchema = StructType(column1 :: column2 :: Nil)

    sparkSession.createDataFrame(
      sparkSession.sparkContext.emptyRDD[Row],
      mySchema
    )
  }

  def getDfWithNRows(sparkSession: SparkSession, n: Int): DataFrame = {
    import sparkSession.implicits._

    (1 to n).toList
      .map { index => (s"$index", s"c1-r$index", s"c2-r$index") }
      .toDF("c0", "c1", "c2")
  }

  def getDfWithNestedColumn(sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._

//    val schema = new StructType()
//      .add("dc_id", StringType)
//      .add("lang", StringType)
//      .add("source",
//        MapType(
//          StringType,
//          new StructType()
//            .add("description", StringType)
//            .add("weblink", StringType)
//            .add("ip", StringType)
//            .add("id", LongType)
//            .add("temp", LongType)
//            .add("c02_level", LongType)
//            .add("geo",
//              new StructType()
//                .add("lat", DoubleType)
//                .add("long", DoubleType)
//            )
//        )
//      )

    // Create a single entry with id and its complex and nested data types
    Seq("""
      {
      "dc_id": "dc-101",
      "lang":"en-us",
      "source": {
          "sensor-igauge": {
            "id": 10,
            "ip": "68.28.91.22",
            "description": "Sensor attached to the container ceilings",
            "weblink": "http://www.testing-url.dk/",
            "temp":35,
            "c02_level": 1475,
            "geo": {"lat":38.00, "long":97.00}
            }
          }
        }
      }""").toDF()
  }

  def getDfMissing(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "f"),
      ("2", "b", "d"),
      ("3", null, "f"),
      ("4", "a", null),
      ("5", "a", "f"),
      ("6", null, "d"),
      ("7", null, "d"),
      ("8", "b", null),
      ("9", "a", "f"),
      ("10", null, null),
      ("11", null, "f"),
      ("12", null, "d")
    ).toDF("item", "att1", "att2")
  }

  def getDfFull(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "c"),
      ("2", "a", "c"),
      ("3", "a", "c"),
      ("4", "b", "d")
    ).toDF("item", "att1", "att2")
  }

  def getDfWithNegativeNumbers(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "-1", "-1.0"),
      ("2", "-2", "-2.0"),
      ("3", "-3", "-3.0"),
      ("4", "-4", "-4.0")
    ).toDF("item", "att1", "att2")
  }

  def getDfCompleteAndInCompleteColumns(
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "f"),
      ("2", "b", "d"),
      ("3", "a", null),
      ("4", "a", "f"),
      ("5", "b", null),
      ("6", "a", "f")
    ).toDF("item", "att1", "att2")
  }

  case class NumericRow(
      item: String,
      completeInteger: Integer,
      completeDouble: Double,
      incompleteInteger: Option[Integer],
      incompleteDouble: Option[Double],
      att1: String
      )

  def getDfCompleteAndInCompleteColumnsNumeric(
      sparkSession: SparkSession
  ): DataFrame = {

    val rows: Seq[NumericRow] = Seq(
      NumericRow("1", 1, 1.01, Some(1), Some(1.01), "f"),
      NumericRow("2", 2, 2.01, None, None, "d"),
      NumericRow("3", 3, 3.01, Some(3), Some(3.01), null),
      NumericRow("4", 4, 4.01, None, None, "f"),
      NumericRow("5", 5, 5.01, Some(5), Some(5.01), null),
      NumericRow("6", 6, 6.01, Some(6), Some(6.01), "f"),
      NumericRow("6", 7, 7.01, Some(7), Some(7.01), "f"),
      NumericRow("6", 8, 8.01, Some(8), Some(8.01), "f"),
      NumericRow("6", 9, 9.01, Some(9), Some(9.01), "f")
    )

    sparkSession.sqlContext.createDataFrame(rows)
  }

  def getDfCompleteAndInCompleteColumnsDelta(
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("7", "a", null),
      ("8", "b", "d"),
      ("9", "a", null)
    ).toDF("item", "att1", "att2")
  }

  def getDfFractionalIntegralTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1.0"),
      ("2", "1")
    ).toDF("item", "att1")
  }

  def getDfFractionalStringTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1.0"),
      ("2", "a")
    ).toDF("item", "att1")
  }

  def getDfIntegralStringTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1"),
      ("2", "a")
    ).toDF("item", "att1")
  }

  def getDfWithNumericValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    // att2 is always bigger than att1
    Seq(
      ("1", 1, 0, 0),
      ("2", 2, 0, 0),
      ("3", 3, 0, 0),
      ("4", 4, 5, 4),
      ("5", 5, 6, 6),
      ("6", 6, 7, 7)
    ).toDF("item", "att1", "att2", "att3")
  }

  def getDfWithNumericFractionalValues(
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._
    Seq(
      ("1", 1.0, 0.0),
      ("2", 2.0, 0.0),
      ("3", 3.0, 0.0),
      ("4", 4.0, 5.0),
      ("5", 5.0, 6.0),
      ("6", 6.0, 7.0)
    ).toDF("item", "att1", "att2")
  }

  def getDfWithNumericFractionalValuesForKLL(
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._
    Seq(
      ("1", 1.0, 0.0),
      ("2", 2.0, 0.0),
      ("3", 3.0, 0.0),
      ("4", 4.0, 5.0),
      ("5", 5.0, 6.0),
      ("6", 6.0, 7.0),
      ("7", 7.0, 0.0),
      ("8", 8.0, 0.0),
      ("9", 9.0, 0.0),
      ("10", 10.0, 5.0),
      ("11", 11.0, 6.0),
      ("12", 12.0, 7.0),
      ("13", 13.0, 0.0),
      ("14", 14.0, 0.0),
      ("15", 15.0, 0.0),
      ("16", 16.0, 5.0),
      ("17", 17.0, 6.0),
      ("18", 18.0, 7.0),
      ("19", 19.0, 0.0),
      ("20", 20.0, 0.0),
      ("21", 21.0, 0.0),
      ("22", 22.0, 5.0),
      ("23", 23.0, 6.0),
      ("24", 24.0, 7.0),
      ("25", 25.0, 0.0),
      ("26", 26.0, 0.0),
      ("27", 27.0, 0.0),
      ("28", 28.0, 5.0),
      ("29", 29.0, 6.0),
      ("30", 30.0, 7.0)
    ).toDF("item", "att1", "att2")
  }

  def getDfWithUniqueColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "0", "3", "1", "5", "0"),
      ("2", "0", "3", "2", "6", "0"),
      ("3", "0", "3", null, "7", "0"),
      ("4", "5", null, "3", "0", "4"),
      ("5", "6", null, "4", "0", "5"),
      ("6", "7", null, "5", "0", "6")
    )
      .toDF(
        "unique",
        "nonUnique",
        "nonUniqueWithNulls",
        "uniqueWithNulls",
        "onlyUniqueWithOtherNonUnique",
        "halfUniqueCombinedWithNonUnique"
      )
  }

  def getDfWithDistinctValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("a", null),
      ("a", null),
      (null, "x"),
      ("b", "x"),
      ("b", "x"),
      ("c", "y")
    )
      .toDF("att1", "att2")
  }

  def getDfWithConditionallyUninformativeColumns(
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._
    Seq(
      (1, 0),
      (2, 0),
      (3, 0)
    ).toDF("att1", "att2")
  }

  def getDfWithConditionallyInformativeColumns(
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._
    Seq(
      (1, 4),
      (2, 5),
      (3, 6)
    ).toDF("att1", "att2")
  }

  def getDfWithCategoricalColumn(
      sparkSession: SparkSession,
      numberOfRows: Int,
      categories: Seq[String]
  ): DataFrame = {

    val random = new Random(0)

    import sparkSession.implicits._
    (1 to numberOfRows).toList
      .map { index => (s"$index", random.shuffle(categories).head) }
      .toDF("att1", "categoricalColumn")
  }

  def getDfWithVariableStringLengthValues(
      sparkSession: SparkSession
  ): DataFrame = {
    import sparkSession.implicits._
    Seq(
      "",
      "a",
      "bb",
      "ccc",
      "dddd"
    ).toDF("att1")
  }

  private[this] def getFakeColumnProfileWithName(
      columnName: String
  ): ColumnProfile = {

    val fakeColumnProfile = mock[ColumnProfile]
    inSequence {
      (fakeColumnProfile.column _)
        .expects()
        .returns(columnName)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  def getFakeColumnProfileWithNameAndCompleteness(
      columnName: String,
      completeness: Double
  ): ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.completeness _)
        .expects()
        .returns(completeness)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  def getFakeColumnProfileWithNameAndApproxNumDistinctValues(
      columnName: String,
      approximateNumDistinctValues: Long
  ): ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.approximateNumDistinctValues _)
        .expects()
        .returns(approximateNumDistinctValues)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  def getFakeColumnProfileWithColumnNameAndDataType(
      columnName: String,
      dataType: DataTypeInstances.Value
  ): ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.dataType _)
        .expects()
        .returns(dataType)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }

  def getFakeColumnProfileWithColumnNameAndHistogram(
      columnName: String,
      histogram: Option[Distribution]
  ): ColumnProfile = {

    val fakeColumnProfile = getFakeColumnProfileWithName(columnName)
    inSequence {
      (fakeColumnProfile.histogram _)
        .expects()
        .returns(histogram)
        .anyNumberOfTimes()
    }

    fakeColumnProfile
  }
}
