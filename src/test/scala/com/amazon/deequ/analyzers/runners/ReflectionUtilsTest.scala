/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.analyzers.runners.ReflectionUtils.getFieldValue
import org.scalatest.WordSpec

class ReflectionUtilsTest extends WordSpec {
  val TestObject = new TestClass("test_name", Some(1))

  "ReflectionUtils" should {
    "return Some(value) when object field exists" in {
      assert(getFieldValue(TestObject, "a") == Some("test_name"))
      assert(getFieldValue(TestObject, "b") == Some(Some(1)))
      assert(getFieldValue(TestObject, "c") == Some(None))
    }

    "return None when object field does not exist" in {
      assert(getFieldValue(TestObject, "d") == None)
    }
  }
}

case class TestClass(a: String, b: Option[Int] = None, c: Option[Double] = None)

