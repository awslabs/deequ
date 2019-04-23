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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.Random


trait FixtureSupport {

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
      ("1", 1, 0),
      ("2", 2, 0),
      ("3", 3, 0),
      ("4", 4, 5),
      ("5", 5, 6),
      ("6", 6, 7)
    ).toDF("item", "att1", "att2")
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

  def getDfWithVariableLengthValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      ("a", -1, 0D),
      ("bb", 222, 0D),
      ("ccc", 333, 0D),
      ("dddd", 4444, 44D)
    ).toDF("strings", "ints", "doubles")
  }
}
