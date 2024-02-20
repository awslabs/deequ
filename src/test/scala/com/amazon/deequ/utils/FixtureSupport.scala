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

package com.amazon.deequ.utils

import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.profiles.NumericColumnProfile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import scala.util.Random


trait FixtureSupport {

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

    sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], mySchema)
  }

  def getDfWithNRows(sparkSession: SparkSession, n: Int): DataFrame = {
    import sparkSession.implicits._

    (1 to n)
      .toList
      .map { index => (s"$index", s"c1-r$index", s"c2-r$index")}
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

  def getDfCompleteAndInCompleteColumns(sparkSession: SparkSession): DataFrame = {
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

  def getDfCompleteAndInCompleteColumnsWithIntId(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      (1, "a", "f"),
      (2, "b", "d"),
      (3, "a", null),
      (4, "a", "f"),
      (5, "b", null),
      (6, "a", "f")
    ).toDF("item", "att1", "att2")
  }

  def getDfCompleteAndInCompleteColumnsWithSpacesInNames(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "ab", "abc1"),
      ("2", "bc", null),
      ("3", "ab", "def2ghi"),
      ("4", "ab", null),
      ("5", "bcd", "ab"),
      ("6", "a", "pqrs")
    ).toDF("some item", "att 1", "att 2")
  }

  def getDfCompleteAndInCompleteColumnsAndVarLengthStrings(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "f", 1, Some(1)),
      ("22", "b", "d", 2, None),
      ("333", "a", null, 3, Some(3)),
      ("4444", "a", "f", 4, Some(4)),
      ("55555", "b", null, 5, None),
      ("666666", "a", "f", 6, Some(6))
    ).toDF("item", "att1", "att2", "val1", "val2")
  }

  def getDateDf(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      (100, "Furniture", "Product 1", 25, null),
      (101, "Cosmetics", "Product 2", 20, "2022-01-05"),
      (102, "Furniture", "Product 3", 30, null),
      (103, "Electronics", "Product 4", 10, null),
      (104, "Electronics", "Product 5", 50, null)
    ).toDF("id", "product", "product_id", "units", "date")
  }
  def getDfCompleteAndInCompleteColumnsDelta(sparkSession: SparkSession): DataFrame = {
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
      ("1", 1, 0, 0, None),
      ("2", 2, 0, 0, None),
      ("3", 3, 0, 0, None),
      ("4", 4, 5, 4, Some(5)),
      ("5", 5, 6, 6, Some(6)),
      ("6", 6, 7, 7, Some(7))
    ).toDF("item", "att1", "att2", "att3", "attNull")
  }

  def getDfWithEscapeCharacters(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    // The names are with escape characters '
    Seq(
      ("'foo'", 50),
      ("Yes This's My Name", 29),
      ("It's foo", 33),
      ("foo", 22),
      ("foo '' name", 22),
      ("'''", 25),
      ("", 25),
      ("Trying !o include: @ll the #$peci@l charac%ers possib^e & test* that (out)~[here] {which} i`s great?\";", 25)
    ).toDF("name", "age")
  }

  def getDfWithNumericFractionalValues(sparkSession: SparkSession): DataFrame = {
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

  def getDfWithNumericFractionalValuesForKLL(sparkSession: SparkSession): DataFrame = {
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
    .toDF("unique", "nonUnique", "nonUniqueWithNulls", "uniqueWithNulls",
      "onlyUniqueWithOtherNonUnique", "halfUniqueCombinedWithNonUnique")
  }

  def getDfWithDistinctValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("a", null),
      ("a", null),
      (null, "x"),
      ("b", "x"),
      ("b", "x"),
      ("c", "y"))
      .toDF("att1", "att2")
  }

  def getDfWithDistinctValuesQuotes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("a", null, "Already Has "),
      ("a", null, " Can't Proceed"),
      (null, "can't", "Already Has "),
      ("b", "help", " Can't Proceed"),
      ("b", "but", "Already Has "),
      ("c", "wouldn't", " Can't Proceed"))
      .toDF("att1", "att2", "reason")
  }

  def getDfWithConditionallyUninformativeColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      (1, 0),
      (2, 0),
      (3, 0)
    ).toDF("att1", "att2")
  }

  def getDfWithConditionallyInformativeColumns(sparkSession: SparkSession): DataFrame = {
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
      categories: Seq[String])
    : DataFrame = {

    val random = new Random(0)

    import sparkSession.implicits._
    (1 to numberOfRows)
      .toList
      .map { index => (s"$index", random.shuffle(categories).head)}
      .toDF("att1", "categoricalColumn")
  }

  def getDfWithVariableStringLengthValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      "",
      "a",
      "bb",
      "ccc",
      "dddd"
    ).toDF("att1")
  }

  def getDfWithStringColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      (0, "India", "Xavier House, 2nd Floor", "St. Peter Colony, Perry Road", "Bandra (West)"),
      (1, "India", "503 Godavari", "Sir Pochkhanwala Road", "Worli"),
      (2, "India", "4/4 Seema Society", "N Dutta Road, Four Bungalows", "Andheri"),
      (3, "India", "1001D Abhishek Apartments", "Juhu Versova Road", "Andheri"),
      (4, "India", "95, Hill Road", null, null),
      (5, "India", "90 Cuffe Parade", "Taj President Hotel", "Cuffe Parade"),
      (6, "India", "4, Seven PM", "Sir Pochkhanwala Rd", "Worli"),
      (7, "India", "1453 Sahar Road", null, null)
    )
      .toDF("id", "Country", "Address Line 1", "Address Line 2", "Address Line 3")
  }

  def getDfWithPeriodInName(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "c"),
      ("2", "a", "c"),
      ("3", "a", "c"),
      ("4", "b", "d")
    ).toDF("item.one", "att1", "att2")
  }

  def getDfForWhereClause(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("Acme", "90210", "CA", "Los Angeles"),
      ("Acme", "90211", "CA", "Los Angeles"),
      ("Robocorp", null, "NJ", null),
      ("Robocorp", null, "NY", "New York")
    ).toDF("Company", "ZipCode", "State", "City")
  }

  def getDfCompleteAndInCompleteColumnsWithPeriod(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "f"),
      ("2", "b", "d"),
      ("3", "a", null),
      ("4", "a", "f"),
      ("5", "b", null),
      ("6", "a", "f")
    ).toDF("item.one", "att.1", "att.2")
  }

  def getDfWithNameAndAge(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      ("foo", 50),
      ("bar", 20)
    ).toDF("name", "age")
  }

  def getFakeNumericColumnProfileWithMinMaxMeanAndStdDev(
    columnName: String,
    completeness: Double,
    dataType: DataTypeInstances.Value,
    minimum: Double,
    maximum: Double,
    mean: Double,
    stdDev: Double
  ): NumericColumnProfile = {

    NumericColumnProfile(
      column = columnName,
      completeness = completeness,
      approximateNumDistinctValues = 1000,
      dataType = dataType,
      isDataTypeInferred = false,
      typeCounts = Map[String, Long](),
      histogram = None,
      kll = None,
      mean = Some(mean),
      maximum = Some(maximum),
      minimum = Some(minimum),
      sum = Some(1000.879),
      stdDev = Some(1.023),
      approxPercentiles = None
    )
  }
}
