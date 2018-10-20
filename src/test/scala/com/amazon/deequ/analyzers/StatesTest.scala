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
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class StatesTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "FrequenciesAndNumRows" should {
    "merge correctly" in withSparkSession { session =>

      import session.implicits._

      val dataA = Seq("A", "A", "B").toDF("att1")
      val dataB = Seq("A", "C", "C").toDF("att1")

      val stateA = FrequencyBasedAnalyzer.computeFrequencies(dataA, "att1" :: Nil)
      val stateB = FrequencyBasedAnalyzer.computeFrequencies(dataB, "att1" :: Nil)

      val stateAB = stateA.sum(stateB)

      println(stateA.frequencies.schema)
      stateA.frequencies.collect().foreach { println }
      println()

      println(stateB.frequencies.schema)
      stateB.frequencies.collect().foreach { println }
      println()

      println(stateAB.frequencies.schema)
      stateAB.frequencies.collect().foreach { println }

      val mergedFrequencies = stateAB.frequencies.collect()
        .map { row => row.getString(0) -> row.getLong(1) }
        .toMap

      assert(mergedFrequencies.size == 3)
      assert(mergedFrequencies.get("A").contains(3))
      assert(mergedFrequencies.get("B").contains(1))
      assert(mergedFrequencies.get("C").contains(2))
    }
  }
}
