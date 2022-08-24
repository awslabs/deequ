/**
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.comparison

import com.amazon.deequ.SparkContextSpec
import org.scalatest.wordspec.AnyWordSpec

class RowCountTest extends AnyWordSpec with SparkContextSpec {
  "Row Count Test" should {
    "row count match equals 0.8" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (3, "Helena")))
      val testDS1 = rdd1.toDF("id", "name")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (5, "Tyler"),
        (6, "Megan")))
      val testDS2 = rdd2.toDF("new_id", "name")

      val ds1 = testDS1
      val ds2 = testDS2

      val assertion: Double => Boolean = _ >= 0.80

      val result = RowCount.rowCount(ds1, ds2, assertion)
      assert(result)
    }

    "row count match equals 1.0" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (1, "Jorge"),
        (3, "Helena")))
      val testDS1 = rdd1.toDF("id", "name")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (5, "Tyler"),
        (6, "Megan")))
      val testDS2 = rdd2.toDF("new_id", "name")

      val ds1 = testDS1
      val ds2 = testDS2

      val assertion: Double => Boolean = _ >= 1.0

      val result = RowCount.rowCount(ds1, ds2, assertion)
      assert(result)
    }

    "row count match match equals 0.6" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena")))
      val testDS1 = rdd1.toDF("id", "name")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (5, "Tyler"),
        (6, "Megan")))
      val testDS2 = rdd2.toDF("new_id", "name")

      val ds1 = testDS1
      val ds2 = testDS2

      val assertion: Double => Boolean = _ >= 0.60

      val result = RowCount.rowCount(ds1, ds2, assertion)
      assert(result)
    }
    "row count match higher than 0.55" in withSparkSession { spark =>
      import spark.implicits._

      val rdd1 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (1, "Jorge"),
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (1, "Jorge"),
        (3, "Helena")))
      val testDS1 = rdd1.toDF("id", "name")

      val rdd2 = spark.sparkContext.parallelize(Seq(
        (1, "John"),
        (2, "Javier"),
        (3, "Helena"),
        (5, "Tyler"),
        (6, "Megan")))
      val testDS2 = rdd2.toDF("new_id", "name")

      val ds1 = testDS1
      val ds2 = testDS2

      val assertion: Double => Boolean = _ >= 0.55

      val result = RowCount.rowCount(ds1, ds2, assertion)
      assert(result)
    }
  }
}
