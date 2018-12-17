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

package com.amazon.deequ
package checks

import com.amazon.deequ.analyzers.{Completeness, Compliance}
import com.amazon.deequ.utils.FixtureSupport
import org.scalatest.{Matchers, WordSpec}

class FilterableCheckTest extends WordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Filterable checks" should {
    "build correctly" in {

      val check = Check(CheckLevel.Error, "someCheck")
        .isComplete("col1")
        .isComplete("col2").where("marketplace = 'EU'")
        .hasCompleteness("col3", _ >= 0.9).where("marketplace = 'NA'")
        .satisfies("someCol > 5", "const1")
        .satisfies("someCol > 10", "const2").where("marketplace = 'EU'")

      val completenessAnalyzers =
        check.requiredAnalyzers()
          .filter { _.isInstanceOf[Completeness] }
          .map { _.asInstanceOf[Completeness] }
          .toArray
          .sortBy { _.column }

      assert(completenessAnalyzers.length == 3)
      assert(completenessAnalyzers.head.where.isEmpty)
      assert(completenessAnalyzers(1).where.contains("marketplace = 'EU'"))
      assert(completenessAnalyzers(2).where.contains("marketplace = 'NA'"))

      val complianceAnalyzers =
        check.requiredAnalyzers()
          .filter { _.isInstanceOf[Compliance] }
          .map { _.asInstanceOf[Compliance] }
          .toArray
          .sortBy { _.instance }

      assert(complianceAnalyzers.length == 2)
      assert(complianceAnalyzers.head.where.isEmpty)
      assert(complianceAnalyzers(1).where.contains("marketplace = 'EU'"))
    }
  }

}
