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

