package com.amazon.deequ.demo

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes.Integral
import com.amazon.deequ.demo.DemoUtils.NEVER

object ReviewChecks {

  val SUGGESTED_CHECK: Check = Check(CheckLevel.Error, "Suggested constraints")
    .isComplete("review_id")
    .isComplete("customer_id")
    .hasDataType("customer_id", Integral)
    .isNonNegative("customer_id")
    .isComplete("helpful_votes")
    .hasDataType("helpful_votes", Integral)
    .isNonNegative("helpful_votes")
    .isComplete("star_rating")
    .hasDataType("star_rating", Integral)
    //.isNonNegative("star_rating")
    .isContainedIn("star_rating", 1.0, 5.0)
    .isComplete("product_title")
    .hasCompleteness("review_headline", _ >= 0.99)
    .isComplete("product_id")
    .isComplete("total_votes")
    .hasDataType("total_votes", Integral)
    .isNonNegative("total_votes")
    .isComplete("product_category")
    .isContainedIn("product_category", Array("Beauty", "Shoes", "Jewelry", "Video Games"))
    .isComplete("product_parent")
    .hasDataType("product_parent", Integral)
    .isNonNegative("product_parent")
    .isComplete("vine")
    .isContainedIn("vine", Array("N", "Y"))
    .isContainedIn("vine", Array("N"), _ >= 0.99)
    .isComplete("marketplace")
    .isContainedIn("marketplace", Array("US"))
    .isComplete("verified_purchase")
    .isContainedIn("verified_purchase", Array("Y", "N"))

  val CUSTOM_CHECK: Check = Check(CheckLevel.Error, "Custom constraints")
    //.containsEmail("review_body", NEVER)
    .containsCreditCardNumber("review_body", NEVER)
    .hasApproxQuantile("star_rating", 0.5, _ >= 4.0)
    .hasMean("star_rating", meanRating => meanRating > 3.5 && meanRating < 5.0)

  //TODO call some service for num distinct products per category, check with HLL sketches

  def createCardinalityCheck(day: String): Check = {

    Check(CheckLevel.Error, "External service check")
      .hasApproxCountDistinct("product_id", { count =>
        val expectedCount = ProductService.getNumAvailableProducts(day, "Beauty")

        count <= 1.2 * expectedCount
      }).where("product_category = 'Beauty'")
      .hasApproxCountDistinct("product_id", { count =>
        val expectedCount = ProductService.getNumAvailableProducts(day, "Video Games")

        count <= 1.2 * expectedCount
      })
      .where("product_category = 'Video Games'")
  }


  //.isContainedIn("review_date", Array("2015-08-01")) PARTITIONING KEY
  //.isUnique("product_id") FAILED ON HOLDOUT SET ALREADY, TOO SMALL SAMPLE FOR TRAINING
  //.isContainedIn("product_category", Array("Beauty", "Shoes", "Jewelry"), _ >= 0.92) MORE GENERIC CONSTRAINT
  //.isUnique("product_parent") FAILED ON HOLDOUT SET
  //.isComplete("review_body") FAILED ON HOLDOUT SET
  //.isUnique("review_body") FAILED ON HOLDOUT SET
}
