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
