package com.amazon.deequ.checks

import com.amazon.deequ.SparkContextSpec
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
