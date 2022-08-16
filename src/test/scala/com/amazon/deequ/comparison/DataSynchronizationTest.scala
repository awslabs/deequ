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

case class rowItems(id: Int, name: String, state: String)

 class DataSynchronizationTest extends FlatSpec{

    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val rdd1 = spark.sparkContext.parallelize(Seq(
      rowItems(1,"John","NY"),
      rowItems(2,"Javier","WI"),
      rowItems(3,"Helena","TX"),
      rowItems(4,"Helena","TX"),
      rowItems(5,"Nick","FL"),
      rowItems(6,"Molly","TX")))
    val testDS1 = rdd1.toDF()

    val rdd2 = spark.sparkContext.parallelize(Seq(
      rowItems(1,"John","NY"),
      rowItems(2,"Javier","WI"),
      rowItems(3,"Helena","TX"),
      rowItems(4,"Helena","TX"),
      rowItems(5,"Helena","FL"),
      rowItems(7,"Megan","TX")))
    val testDS2 = rdd2.toDF()

  it should "match 0.66 when id is colKey and name is compCols" in {
    val ds1 = testDS1
    val ds2 = testDS2
    val colKeyMap = Map("id" -> "id")
    val compCols = Some(Map("name" -> "name"))
    val assertion: Double => Boolean = _ >= 0.60

    val result = (DataSynchronization.columnMatch(ds1,ds2,colKeyMap,compCols,assertion))
    assert(result)
  }

  it should "match 0.83 when id is colKey and state is compCols" in {
    val ds1 = testDS1
    val ds2 = testDS2
    val colKeyMap = Map("id" -> "id")
    val compCols = Some(Map("state" -> "state"))
    val assertion: Double => Boolean = _ >= 0.80

    val result = (DataSynchronization.columnMatch(ds1,ds2,colKeyMap,compCols,assertion))
    assert(result)
  }

  it should "return false because col name isn't unique" in {
    val ds1 = testDS1
    val ds2 = testDS2
    val colKeyMap = Map("name" -> "name")
    val compCols = Some(Map("state" -> "state"))
    val assertion: Double => Boolean = _ >= 0.66

    val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
    assert(!result)
  }

  it should "match 0.66 when id is unique col, name and state are compCols" in {
    val ds1 = testDS1
    val ds2 = testDS2
    val colKeyMap = Map("id" -> "id")
    val compCols = Some(Map("name" -> "name", "state" -> "state"))
    val assertion: Double => Boolean = _ >= 0.60

    val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
    assert(result)
  }

  it should "match 0.66 (same test as above only the data sets change)" in {
    val ds1 = testDS2
    val ds2 = testDS1
    val colKeyMap = Map("id" -> "id")
    val compCols = Some(Map("name" -> "name", "state" -> "state"))
    val assertion: Double => Boolean = _ >= 0.40

    val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
    assert(result)
  }

  it should "return false because the id col in ds1 isn't unique" in {
    val rdd3 = spark.sparkContext.parallelize(Seq(
      rowItems(1,"John","NY"),
      rowItems(1,"Javier","WI"),
      rowItems(3,"Helena","TX"),
      rowItems(4,"Helena","TX"),
      rowItems(5,"Helena","FL"),
      rowItems(9,"Nicholas","CT"),
      rowItems(7,"Megan","TX")))

    val testDS3 = rdd3.toDF()

    val ds1 = testDS3
    val ds2 = testDS2
    val colKeyMap = Map("id" -> "id")
    val compCols = Some(Map("name" -> "name", "state" -> "state"))
    val assertion: Double => Boolean = _ >= 0.40

    val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
    assert(!result)
  }

  it should "return false because the id col in ds2 isn't unique" in {
    val rdd3 = spark.sparkContext.parallelize(Seq(
      rowItems(1,"John","NY"),
      rowItems(1,"Javier","WI"),
      rowItems(3,"Helena","TX"),
      rowItems(4,"Helena","TX"),
      rowItems(5,"Helena","FL"),
      rowItems(5,"Nicholas","CT"),
      rowItems(7,"Megan","TX")))

    val testDS3 = rdd3.toDF()

    val ds1 = testDS1
    val ds2 = testDS3
    val colKeyMap = Map("id" -> "id")
    val compCols = Some(Map("name" -> "name", "state" -> "state"))
    val assertion: Double => Boolean = _ >= 0.40

    val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
    assert(!result)
  }

  it should "return false because col state isn't unique" in {
    val ds1 = testDS1
    val ds2 = testDS2
    val colKeyMap = Map("state" -> "state")
    val compCols = Some(Map("name" -> "name"))
    val assertion: Double => Boolean = _ >= 0.66

    val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
    assert(!result)
  }

   it should "check all columns and return an assertion of .66" in {
     val ds1 = testDS1
     val ds2 = testDS2
     val colKeyMap = Map("id" -> "id")
     val compCols = None
     val assertion: Double => Boolean = _ >= 0.66

     val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
     assert(result)
   }

   it should "return false because state column isn't unique" in {
     val ds1 = testDS1
     val ds2 = testDS2
     val colKeyMap = Map("state" -> "state")
     val compCols = None
     val assertion: Double => Boolean = _ >= 0.66

     val result = (DataSynchronization.columnMatch(ds1, ds2, colKeyMap, compCols, assertion))
     assert(!result)
   }
}