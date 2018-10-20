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

package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{Matchers, WordSpec}
import org.apache.spark.sql.functions.{expr, rand}

class StateAggregationTests extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "State aggregation outside" should {

    "give correct results" in withSparkSession { session =>

      correctlyAggregatesStates(session, Size())
      correctlyAggregatesStates(session, Uniqueness("attribute" :: "value" :: Nil))
      correctlyAggregatesStates(session, Distinctness("attribute" :: Nil))
      correctlyAggregatesStates(session, CountDistinct("value" :: Nil))
      correctlyAggregatesStates(session, UniqueValueRatio("attribute" :: "value" :: Nil))
      correctlyAggregatesStates(session, Completeness("attribute"))
      correctlyAggregatesStates(session, Compliance("attribute", "attribute like '%facets%'"))
      correctlyAggregatesStates(session, ApproxCountDistinct("attribute"))
      correctlyAggregatesStates(session, MutualInformation("numbersA", "numbersB"))
      correctlyAggregatesStates(session, Correlation("numbersA", "numbersB"))
    }
  }

  def correctlyAggregatesStates[S <: State[S]](
      session: SparkSession,
      analyzer: Analyzer[S, Metric[_]])
    : Unit = {

    val dataA = initialData(session)
    val dataB = deltaData(session)
    val dataAB = dataA union dataB

    val stateA = analyzer.computeStateFrom(dataA)
    val stateB = analyzer.computeStateFrom(dataB)

    val metricFromCalculate = analyzer.calculate(dataAB)
    val mergedState = Analyzers.merge(stateA, stateB)

    val metricFromAggregation = analyzer.computeMetricFrom(mergedState)

    assert(metricFromAggregation == metricFromCalculate)
  }

  def initialData(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      (1, "B00BJXTG66", "2nd story llc-0-$ims_facets-0-", "extended"),
      (1, "B00BJXTG66", "2nd story llc-0-value", "Intimate Organics"),
      (1, "B00DLT13JY", "Binding-0-$ims_facets-0-", "extended"),
      (1, "B00ICANXP4", "Binding-0-$ims_facets-0-", "extended"),
      (1, "B00MG1DSWI", "Binding-0-$ims_facets-0-", "extended"),
      (1, "B00DLT13JY", "Binding-0-value", "consumer_electronics"),
      (1, "B00ICANXP4", "Binding-0-value", "pc"),
      (1, "B00MG1DSWI", "Binding-0-value", "toy"),
      (1, "B0012P3IYC", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001FFEJY2", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001GF63ZO", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RKKJRQ", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RLFZTW", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RMNV6A", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RPWK9G", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RQ37ME", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RQJHVE", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RRX642", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RS3C2C", "CATEGORY-0-$ims_facets-0-", "extended"),
      (1, "B001RTDRO4", "CATEGORY-0-$ims_facets-0-", "extended"))
      .toDF("marketplace_id", "item", "attribute", "value")
      .withColumn("numbersA", rand(0xbeefcafe))
      .withColumn("numbersB", rand(0xdeadbeef))
  }

  def deltaData(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      (1, "B008FZTBAW", "BroadITKitem_type_keyword-0-", "jewelry-products"),
      (1, "B00BUU5R02", "BroadITKitem_type_keyword-0-", "kitchen-products"),
      (1, "B0054UJNJK", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00575Q69M", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B005F2OSTC", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00BQNCQWU", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00BQND3WC", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1CU3PC", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1CYE66", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1CYIKS", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1CZ2NK", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1D26SI", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1D2HQ4", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1D554A", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1D5I0Q", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1D5LU8", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1D927G", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1DAXMO", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00C1DDNC6", "BroadITKitem_type_keyword-0-", "lighting-products"),
      (1, "B00CF0URZ6", "BroadITKitem_type_keyword-0-", "lighting-products"))
      .toDF("marketplace_id", "item", "attribute", "value")
      .withColumn("numbersA", rand(0xcafebeef))
      .withColumn("numbersB", rand(0xcafecafe))
  }
}
