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

import org.scalatest.{Matchers, WordSpec}

class SparkBasicTest extends WordSpec with Matchers with SparkContextSpec {
  "check that initializing a spark context and a basic example works" in
    withSparkSession { sparkSession =>
      val sc = sparkSession.sparkContext
      val xs = sc.parallelize(1 to 100)
      val res = xs.sum()
      res should be(5050)
    }

  "check that monitoring spark session works" in
    withMonitorableSparkSession { (sparkSession, sparkMonitor) =>
      val sc = sparkSession.sparkContext
      val xs = sc.parallelize(1 to 100)


      (1 to 2).foreach { index =>
        val res = sparkMonitor.withMonitoringSession { stat =>
          val sum = xs.map(_ * index).sum()
          // Spark jobs are running in different monitoring sessions
          assert(stat.jobCount == 1)
          sum
        }
        res should be(5050 * index)
      }

      sparkMonitor.withMonitoringSession { stat =>
        (1 to 2).foreach { index =>
          xs.map(_ * index).sum()
        }
        // Spark jobs are running in the same monitoring session
        assert(stat.jobCount == 2)
      }
    }
}

