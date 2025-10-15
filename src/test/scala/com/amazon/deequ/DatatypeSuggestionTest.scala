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

import com.amazon.deequ.profiles.StringColumnProfile
import com.amazon.deequ.profiles.ColumnProfiler
import com.amazon.deequ.utils.FixtureSupport
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DatatypeSuggestionTest extends AnyWordSpec with Matchers with SparkContextSpec
  with FixtureSupport with MockFactory{

  "Column Profiler" should {
    "return the correct datatype(String) in case of profiling empty string columns" in
      withSparkSession { sparkSession =>

        val df = getEmptyColumnDataDf(sparkSession = sparkSession)

        val profile = ColumnProfiler
                        .profile(df, Option(Seq("att1")))
                        .profiles("att1")

        assert(profile.isInstanceOf[StringColumnProfile])
        assert(profile.isDataTypeInferred && profile.dataType.toString.equalsIgnoreCase("String"))
      }
  }
}
