package com.amazon.deequ.anomalydetection

import org.scalatest.{Matchers, WordSpec}

class AnomalyDetectionTestUtilsTest extends WordSpec with Matchers {

  "AnomalyDetectionTestUtilsTest" should {

    "throw an exception if no value found" in {
      intercept[IllegalArgumentException] {
        AnomalyDetectionTestUtils.firstDoubleFromString("noNumber")
      }
      intercept[IllegalArgumentException] {
        AnomalyDetectionTestUtils.firstThreeDoublesFromString("noNumber")
      }
    }

    "find first value" in {
      val str = "xx3.141yyu4.2"
      val value = AnomalyDetectionTestUtils.firstDoubleFromString(str)
      assert(value == 3.141)
    }

    "find all 3 values" in {
      val str = "In this 1 string are 3.000 values, not 42.01"

      val (first, second, third) = AnomalyDetectionTestUtils.firstThreeDoublesFromString(str)
      assert(first === 1)
      assert(second === 3.0)
      assert(third === 42.01)
    }
  }
}
