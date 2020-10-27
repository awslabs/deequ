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
