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

import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.connect.proto.{
  AllowedValuesSpec,
  CheckLevel => ProtoCheckLevel,
  Check => ProtoCheck,
  ColumnAssertionSpec,
  ColumnSpec,
  ColumnsAssertionSpec,
  ColumnsSpec,
  Constraint => ProtoConstraint,
  PairColumnsAssertionSpec,
  PatternAssertionSpec,
  Predicate => ProtoPredicate,
  PrimaryKeySpec,
  QuantileAssertionSpec,
  SatisfiesSpec,
  SizeSpec
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CheckBuilderTest extends AnyWordSpec with Matchers {

  // ---- helpers --------------------------------------------------------------

  private def gte(threshold: Double): ProtoPredicate =
    ProtoPredicate.newBuilder()
      .setComparison(
        ProtoPredicate.Comparison.newBuilder()
          .setOp(ProtoPredicate.CompareOp.COMPARE_OP_GE)
          .setValue(threshold))
      .build()

  private def between(lower: Double, upper: Double): ProtoPredicate =
    ProtoPredicate.newBuilder()
      .setRange(ProtoPredicate.Range.newBuilder().setLower(lower).setUpper(upper))
      .build()

  // ---- Tests ---------------------------------------------------------------

  "CheckBuilder.build" should {

    "map CHECK_LEVEL_ERROR / WARNING to the corresponding Deequ CheckLevel" in {
      val errorCheck = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("e")
        .build()
      CheckBuilder.build(errorCheck).level shouldBe CheckLevel.Error

      val warnCheck = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_WARNING)
        .setDescription("w")
        .build()
      CheckBuilder.build(warnCheck).level shouldBe CheckLevel.Warning
    }

    "throw on CHECK_LEVEL_UNSPECIFIED rather than silently picking Error" in {
      val unspecified = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_UNSPECIFIED)
        .setDescription("u")
        .build()
      an[IllegalArgumentException] should be thrownBy CheckBuilder.build(unspecified)
    }

    "build an isComplete constraint from a ColumnSpec arm" in {
      val constraint = ProtoConstraint.newBuilder()
        .setIsComplete(ColumnSpec.newBuilder().setColumn("id"))
        .build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(constraint)
        .build()

      val built = CheckBuilder.build(check)
      built.constraints should have size 1
      built.constraints.head.toString should include("Completeness(id")
    }

    "build hasCompleteness from a ColumnAssertionSpec with a Comparison predicate" in {
      val constraint = ProtoConstraint.newBuilder()
        .setHasCompleteness(
          ColumnAssertionSpec.newBuilder()
            .setColumn("email")
            .setAssertion(gte(0.95)))
        .build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(constraint)
        .build()

      noException should be thrownBy CheckBuilder.build(check)
    }

    "build hasMean with a Range (BETWEEN) predicate" in {
      val constraint = ProtoConstraint.newBuilder()
        .setHasMean(
          ColumnAssertionSpec.newBuilder()
            .setColumn("amount")
            .setAssertion(between(100.0, 500.0)))
        .build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(constraint)
        .build()

      noException should be thrownBy CheckBuilder.build(check)
    }

    "thread the where filter into the constraint when set" in {
      val constraint = ProtoConstraint.newBuilder()
        .setWhere("status = 'active'")
        .setIsComplete(ColumnSpec.newBuilder().setColumn("id"))
        .build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(constraint)
        .build()

      val built = CheckBuilder.build(check)
      built.constraints.head.toString should include("status = 'active'")
    }

    "throw when a Predicate has no body (BODY_NOT_SET)" in {
      val constraint = ProtoConstraint.newBuilder()
        .setHasCompleteness(
          ColumnAssertionSpec.newBuilder()
            .setColumn("email")
            .setAssertion(ProtoPredicate.getDefaultInstance))
        .build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(constraint)
        .build()

      an[IllegalArgumentException] should be thrownBy CheckBuilder.build(check)
    }

    "throw when a Constraint has no body (BODY_NOT_SET)" in {
      val emptyConstraint = ProtoConstraint.newBuilder().build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(emptyConstraint)
        .build()

      an[IllegalArgumentException] should be thrownBy CheckBuilder.build(check)
    }

    "build isContainedIn from an AllowedValuesSpec" in {
      val constraint = ProtoConstraint.newBuilder()
        .setIsContainedIn(
          AllowedValuesSpec.newBuilder()
            .setColumn("status")
            .addAllowedValues("active")
            .addAllowedValues("inactive")
            .setAssertion(gte(1.0)))
        .build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(constraint)
        .build()

      noException should be thrownBy CheckBuilder.build(check)
    }

    "build pair-column constraints from PairColumnsAssertionSpec" in {
      val constraint = ProtoConstraint.newBuilder()
        .setIsLessThan(
          PairColumnsAssertionSpec.newBuilder()
            .setColumnA("a")
            .setColumnB("b")
            .setAssertion(gte(1.0)))
        .build()
      val check = ProtoCheck.newBuilder()
        .setLevel(ProtoCheckLevel.CHECK_LEVEL_ERROR)
        .setDescription("c")
        .addConstraints(constraint)
        .build()

      noException should be thrownBy CheckBuilder.build(check)
    }
  }
}
