/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 */

package com.amazon.deequ.connect

import com.amazon.deequ.analyzers.{
  ApproxQuantile,
  Compliance,
  Histogram,
  MutualInformation,
  Size
}
import com.amazon.deequ.connect.proto.{
  Analyzer => ProtoAnalyzer,
  ApproxQuantileSpec,
  ColumnAnalyzerSpec,
  ColumnsAnalyzerSpec,
  ComplianceAnalyzerSpec,
  EmptySpec,
  HistogramSpec
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AnalyzerBuilderTest extends AnyWordSpec with Matchers {

  "AnalyzerBuilder.build" should {

    "build Size with `where` threaded through" in {
      val msg = ProtoAnalyzer.newBuilder()
        .setWhere("active = true")
        .setSize(EmptySpec.getDefaultInstance)
        .build()
      val built = AnalyzerBuilder.build(msg)
      built shouldBe a[Size]
      built.asInstanceOf[Size].where shouldBe Some("active = true")
    }

    "build Histogram with where threaded through (regression: PR 745)" in {
      val msg = ProtoAnalyzer.newBuilder()
        .setWhere("region = 'US'")
        .setHistogram(
          HistogramSpec.newBuilder().setColumn("category").setMaxDetailBins(50))
        .build()
      val built = AnalyzerBuilder.build(msg)
      built shouldBe a[Histogram]
      built.asInstanceOf[Histogram].where shouldBe Some("region = 'US'")
      built.asInstanceOf[Histogram].maxDetailBins shouldBe 50
    }

    "build MutualInformation with where threaded through (regression: PR 745)" in {
      val msg = ProtoAnalyzer.newBuilder()
        .setWhere("year = 2024")
        .setMutualInformation(
          ColumnsAnalyzerSpec.newBuilder().addColumns("a").addColumns("b"))
        .build()
      val built = AnalyzerBuilder.build(msg)
      built shouldBe a[MutualInformation]
      built.asInstanceOf[MutualInformation].where shouldBe Some("year = 2024")
    }

    "build ApproxQuantile with where threaded through (regression: PR 745)" in {
      val msg = ProtoAnalyzer.newBuilder()
        .setWhere("score > 0")
        .setApproxQuantile(
          ApproxQuantileSpec.newBuilder()
            .setColumn("score")
            .setQuantile(0.5))
        .build()
      val built = AnalyzerBuilder.build(msg)
      built shouldBe a[ApproxQuantile]
      built.asInstanceOf[ApproxQuantile].where shouldBe Some("score > 0")
      built.asInstanceOf[ApproxQuantile].quantile shouldBe 0.5
    }

    "build Compliance with explicit columns for precondition checking " +
      "(regression: PR 745)" in {
      val msg = ProtoAnalyzer.newBuilder()
        .setCompliance(
          ComplianceAnalyzerSpec.newBuilder()
            .setInstance("adult")
            .setPredicate("age >= 18")
            .addColumns("age"))
        .build()
      val built = AnalyzerBuilder.build(msg)
      built shouldBe a[Compliance]
      val c = built.asInstanceOf[Compliance]
      c.instance shouldBe "adult"
      c.predicate shouldBe "age >= 18"
      c.columns shouldBe List("age")
    }

    "build a single-column analyzer (Mean) from ColumnAnalyzerSpec" in {
      val msg = ProtoAnalyzer.newBuilder()
        .setMean(ColumnAnalyzerSpec.newBuilder().setColumn("amount"))
        .build()
      val built = AnalyzerBuilder.build(msg)
      built shouldBe a[com.amazon.deequ.analyzers.Mean]
      built.asInstanceOf[com.amazon.deequ.analyzers.Mean].column shouldBe "amount"
    }

    "throw when an Analyzer has no body (BODY_NOT_SET)" in {
      val emptyMsg = ProtoAnalyzer.newBuilder().build()
      an[IllegalArgumentException] should be thrownBy AnalyzerBuilder.build(emptyMsg)
    }
  }
}
