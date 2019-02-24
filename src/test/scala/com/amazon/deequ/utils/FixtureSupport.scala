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

import com.amazon.deequ.repository.SimpleResultSerde
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.Assertions

import scala.util.Random


trait FixtureSupport extends Assertions {

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
    ).toDF("item", "]att1[", "att2")
  }

  def getDfFull(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "a", "c"),
      ("2", "a", "c"),
      ("3", "a", "c"),
      ("4", "b", "d")
    ).toDF("item", "]att1[", "att2")
  }

  def getDfWithNegativeNumbers(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "-1", "-1.0"),
      ("2", "-2", "-2.0"),
      ("3", "-3", "-3.0"),
      ("4", "-4", "-4.0")
    ).toDF("item", "]att1[", "att2")
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
    ).toDF("item", "]att1[", "att2")
  }

  def getDfCompleteAndInCompleteColumnsDelta(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("7", "a", null),
      ("8", "b", "d"),
      ("9", "a", null)
    ).toDF("item", "]att1[", "att2")
  }


  def getDfFractionalIntegralTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1.0"),
      ("2", "1")
    ).toDF("item", "]att1[")
  }

  def getDfFractionalStringTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1.0"),
      ("2", "a")
    ).toDF("item", "]att1[")
  }

  def getDfIntegralStringTypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    Seq(
      ("1", "1"),
      ("2", "a")
    ).toDF("item", "]att1[")
  }

  def getDfWithNumericValues(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    // att2 is always bigger than ]att1[
    Seq(
      ("1", 1, 0),
      ("2", 2, 0),
      ("3", 3, 0),
      ("4", 4, 5),
      ("5", 5, 6),
      ("6", 6, 7)
    ).toDF("item", "]att1[", "att2")
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
    ).toDF("item", "]att1[", "att2")
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
      .toDF("]att1[", "att2")
  }

  def getDfWithConditionallyUninformativeColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      (1, 0),
      (2, 0),
      (3, 0)
    ).toDF("]att1[", "att2")
  }

  def getDfWithConditionallyInformativeColumns(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq(
      (1, 4),
      (2, 5),
      (3, 6)
    ).toDF("]att1[", "att2")
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
      .toDF("]att1[", "categoricalColumn")
  }

  /**
    * A map ordering to ensure that JSON array deserialization results are
    * checked in a consistent manner.
    *
    * In general, an `Ordering[Map[String,Any]]` is not a great pattern: here,
    * however, we have a need to check that various JSON deserialization-based
    * tests correctly parse & handle arrays of objects. Instead of ensuring
    * that the ordering inherint in the element processing of the various bits
    * of code under test do not permute their outputs (something that _is not_
    * a part of any of the code's stated contracts), the idea here is to re-use
    * _this_ ordering definition.
    *
    * This JSON map ordering definiton is specific to the deequ tests: the calls
    * insde the over-loaded `compare` method have a specific, deterministic
    * order of hard-coded keys to check when determining which of two JSON
    * deserialized objects is before another in a sorted collection.
    */
  implicit object OrderingTestMap extends Ordering[Map[String, Any]] {

    type T = Map[String, Any]

    @inline
    private[this] def get(key: String)(x: T): String =
      x.get(key).fold("")(_.asInstanceOf[String])

    private[this] val instance = get("instance") _
    private[this] val name = get("name") _
    private[this] val columnName = get("column_name") _

    override def compare(x: T, y: T): Int = {
      val cmp1 = instance(x).compareTo(instance(y))
      if (cmp1 == 0) {
        val cmp2 = name(x).compareTo(name(y))
        if (cmp2 == 0) {
          columnName(x).compareTo(columnName(y))
        } else {
          cmp2
        }
      } else {
        cmp1
      }
    }
  }

  /**
    * Asserts that the deserialization of two JSON strings results in
    * equivalent arrays of objects.
    * */
  def assertSameJson(jsonA: String, jsonB: String): Unit = {
    val a = SimpleResultSerde.deserialize(jsonA)
    val b = SimpleResultSerde.deserialize(jsonB)
    assert(a.sorted == b.sorted)
  }
}
