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

import org.apache.spark
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}


case class rowItem(id: Int, name: String, state: String)
case class newItem (new_id: Int, name: String, state: String)

class ReferentialIntegrityTest extends FlatSpec {

  def readCsv(sparkSession: SparkSession, filePath: String): DataFrame =
    sparkSession.read.option("header", value = true).
      option("delimiter","\t").csv(filePath).toDF()

  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  val rdd1 = spark.sparkContext.parallelize(Seq(
    rowItem(1, "John", "NY"),
    rowItem(2, "Javier", "WI"),
    rowItem(3, "Helena", "TX"),
    rowItem(3, "Helena", "TX")))
  val testDS1 = rdd1.toDF()

  val rdd2 = spark.sparkContext.parallelize(Seq(
    newItem(1, "John", "NY"),
    newItem(2, "Javier", "WI"),
    newItem(3, "Helena", "TX"),
    newItem(5, "Tyler", "FL"),
    newItem(6, "Megan", "TX")))
  val testDS2 = rdd2.toDF()


  // Expect a 1.0 assertion check of id/new_id columns when testDS1 is the subset
  it should "id matches fully" in {
    val ds1 = testDS1
    val col1 = "id"
    val ds2 = testDS2
    val col2 = "new_id"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 1.0

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(result)
  }

  // Expect a 0.6 assertion of the new_id/id columns when testDS2 is the subset
  it should "id match equal or over 0.60" in {
    val ds1 = testDS2
    val col1 = "new_id"
    val ds2 = testDS1
    val col2 = "id"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 0.6

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(result)
  }

  // Expect a 0.75 assertion check of name columns when testDS1 is the subset
  it should "name match equal or over 0.75" in {
    val ds1 = testDS1
    val col1 = "name"
    val ds2 = testDS2
    val col2 = "name"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 0.75

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(result)
  }

  // Expect a 0.60 assertion of the name columns when testDS2 is the subset
  it should "names match equal or over 0.60" in {
    val ds1 = testDS2
    val col1 = "name"
    val ds2 = testDS1
    val col2 = "name"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 0.60

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(result)
  }

  // Expect a 1.0 assertion of the state columns when testDS1 is the subset
  it should "state match equal or over 1.0" in {
    val ds1 = testDS1
    val col1 = "state"
    val ds2 = testDS2
    val col2 = "state"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 1.0

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(result)
  }

  //Expect a 0.80 assertion of the state columns when testDS2 is the subset
  it should "state match equal or over 0.80" in {
    val ds1 = testDS2
    val col1 = "state"
    val ds2 = testDS1
    val col2 = "state"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 0.8

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(result)
  }

  //Expect a 0.0 assertion of the state column with name column when executed
  it should "state match with name is 0.0" in {
    val ds1 = testDS1
    val col1 = "name"
    val ds2 = testDS2
    val col2 = "state"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 0.0

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(result)
  }


  //Expect failure because col name doesn't exist
  it should "creat an error, col name doesn't exist " in {
    val ds1 = testDS1
    val col1 = "ids"
    val ds2 = testDS2
    val col2 = "new_id"

    //(a: Double) => a can be _
    val assertion: Double => Boolean = (a: Double) => a >= 0.6

    val result = ReferentialIntegrity.subsetCheck(ds1,col1,ds2,col2,assertion)
    assert(!result)
  }



  /* This is an example test
  it should "run" in {
    val x = 1
    val y = 2
    val expected = 3
    assert(expected == ReferentialIntegrity.add(x,y))
  }
   */
}
