package com.amazon.deequ

import com.amazon.deequ.profiles.{ColumnProfiler, ColumnProfiles, StandardColumnProfile}
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

class DatatypeSuggestionTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport with MockFactory{

  "Column Profiler" should {
    "return the correct datatype(String) in case of profiling empty string columns" in
      withSparkSession { sparkSession =>

        val df = getEmptyColumnDataDf(sparkSession = sparkSession)

        val profile = ColumnProfiler
                        .profile(df, Option(Seq("att1")))
                        .profiles("att1")

        assert(profile.isInstanceOf[StandardColumnProfile])
        assert(profile.isDataTypeInferred && profile.dataType.toString.equalsIgnoreCase("String"))
      }
  }

}
