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

package com.amazon.deequ.schema

import com.amazon.deequ.SparkContextSpec
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.scalatest.WordSpec

class RowLevelSchemaValidatorTest extends WordSpec with SparkContextSpec {

  "row level schema validation" should {

    "correctly enforce null constraints" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val data = Seq(
        ("123", "Product A", "2012-07-22 22:59:59"),
        ("N/A", "Product B", null),
        ("456", null, "2012-07-22 22:59:59"),
        (null, "Product C", "2012-07-22 22:59:59")
      ).toDF("id", "name", "event_time")

      val schema = RowLevelSchema()
        .withIntColumn("id", isNullable = false)
        .withStringColumn("name", maxLength = Some(10))
        .withTimestampColumn("event_time", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false)

      val result = RowLevelSchemaValidator.validate(data, schema)

      assert(result.numValidRows == 2)
      val validIds = result.validRows.select("id").collect.map { _.getInt(0) }.toSet
      assert(validIds.size == result.numValidRows)
      assert(validIds.contains(123))
      assert(validIds.contains(456))

      assert(result.numInvalidRows == 2)
      val invalidIds = result.invalidRows.select("id").collect.map { _.getString(0) }.toSet
      assert(invalidIds.size == result.numInvalidRows)
      assert(!invalidIds.contains("123"))
      assert(!invalidIds.contains("456"))
    }

    "correctly enforce string constraints" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val data = Seq(
        "Hello",
        "H.",
        "Hello World",
        "Spaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaam",
        null
      ).toDF("name")

      val schema = RowLevelSchema()
        .withStringColumn(name = "name", isNullable = false, minLength = Some(3),
          maxLength = Some(11))

      val result = RowLevelSchemaValidator.validate(data, schema)

      assert(result.numValidRows == 2)
      val validIds = result.validRows.select("name").collect.map { _.getString(0) }.toSet
      assert(validIds.size == result.numValidRows)
      assert(validIds.contains("Hello"))
      assert(validIds.contains("Hello World"))

      assert(result.numInvalidRows == 3)
      assert(result.invalidRows.count() == result.numInvalidRows)
    }

    "correctly filter string columns according to regexes" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val data = Seq(
        "Hello",
        "hello",
        "hello123",
        "hello world",
        "Spaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaam",
        "&&%%%/&/&/&asdaf",
        null
      ).toDF("name")

      val schema = RowLevelSchema()
        .withStringColumn(name = "name", matches = Some("^[a-z0-9_\\-\\s]+$"))

      val result = RowLevelSchemaValidator.validate(data, schema)

      assert(result.numValidRows == 4)
      val validNames = result.validRows.select("name").collect.map { _.getString(0) }.toSet
      assert(validNames.size == result.numValidRows)
      assert(validNames.contains("hello"))
      assert(validNames.contains("hello123"))
      assert(validNames.contains("hello world"))
      assert(validNames.contains(null))

      val invalidNames = result.invalidRows.select("name").collect.map { _.getString(0) }.toSet
      assert(result.numInvalidRows == 3)
      assert(validNames.intersect(invalidNames).isEmpty)
      assert(result.invalidRows.count() == result.numInvalidRows)
    }

    "correctly enforce integer constraints" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val data = Seq(
        "123",
        "N/A",
        "456",
        "999999",
        "-9",
        "-100000",
        null
      ).toDF("id")

      val schema = RowLevelSchema()
        .withIntColumn("id", isNullable = false, minValue = Some(-10), maxValue = Some(1000))

      val result = RowLevelSchemaValidator.validate(data, schema)

      assert(result.numValidRows == 3)
      val validIds = result.validRows.select("id").collect.map { _.getInt(0) }.toSet
      assert(validIds.size == result.numValidRows)
      assert(validIds.contains(123))
      assert(validIds.contains(456))
      assert(validIds.contains(-9))

      assert(result.numInvalidRows == 4)
      assert(result.invalidRows.count() == result.numInvalidRows)
    }

    "correctly enforce decimal constraints" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val data = Seq(
        "299.000",
        "1295",
        "###",
        "-19.99",
        "-99.99",
        "n/a",
        null
      ).toDF("amount")

      val schema = RowLevelSchema()
         .withDecimalColumn("amount", precision = 10, scale = 2, isNullable = false)

      val result = RowLevelSchemaValidator.validate(data, schema)

      assert(result.numValidRows == 4)
      val validAmounts = result.validRows.collect().map { _.getDecimal(0) }.toSet
      assert(validAmounts.size == result.numValidRows)
      assert(validAmounts.contains(new java.math.BigDecimal("299.00")))
      assert(validAmounts.contains(new java.math.BigDecimal("1295.00")))
      assert(validAmounts.contains(new java.math.BigDecimal("-19.99")))
      assert(validAmounts.contains(new java.math.BigDecimal("-99.99")))

      assert(result.numInvalidRows == 3)
    }

    "correctly enforce timestamp constraints" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val data = Seq(
        "2012-07-22 22:59:59",
        "N/A",
        "2012-07-22 22:21:59",
        "yesterday night",
        null
      ).toDF("created")

      val schema = RowLevelSchema()
        .withTimestampColumn("created", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false)

      val result = RowLevelSchemaValidator.validate(data, schema)

      assert(result.numValidRows == 2)
      assert(result.validRows.count == result.numValidRows)

      assert(result.numInvalidRows == 3)
      assert(result.invalidRows.count() == result.numInvalidRows)
      val invalid = result.invalidRows.select("created").collect.map { _.getString(0) }.toSet
      assert(invalid.contains("N/A"))
      assert(invalid.contains("yesterday night"))
      assert(invalid.contains(null))
    }

    "pass a simple integration test" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val data = Seq(
        ("123", "Product A", "2012-07-22 22:59:59"),
        ("N/A", "Product B", null),
        (null, "Product C", null),
        ("456", "Product D, a must buy", "2012-07-22 22:59:59"),
        ("789", "Product D, another must buy", "2012-07-22 22:59:59"),
        ("101", "Product E", "2012-07-22 22:59:59"),
        ("103", "Product F", "yesterday morning")
      ).toDF("id", "name", "event_time")

      val schema = RowLevelSchema()
        .withIntColumn("id", isNullable = false)
        .withStringColumn("name", maxLength = Some(10))
        .withTimestampColumn("event_time", mask = "yyyy-MM-dd HH:mm:ss")


      val result = RowLevelSchemaValidator.validate(data, schema)


      assert(result.validRows.schema.exists { column =>
        column.name == "id" && column.dataType == IntegerType })
      assert(result.validRows.schema.exists { column =>
        column.name == "name" && column.dataType == StringType })
      assert(result.numValidRows == 2)

      val validNames = result.validRows.collect().map { _.getAs[String]("name") }

      assert(validNames.length == result.numValidRows)
      assert(validNames.contains("Product A"))
      assert(validNames.contains("Product E"))

      val fieldsByName = result.validRows.schema.fields.map { field => field.name -> field }.toMap

      /* Ensure that the data was casted correctly */
      assert(fieldsByName.size == 3)
      assert(fieldsByName("id").dataType == IntegerType)
      assert(fieldsByName("name").dataType == StringType)
      assert(fieldsByName("event_time").dataType == TimestampType)

      assert(result.invalidRows.schema.exists { column =>
        column.name == "id" && column.dataType == StringType })
      assert(result.invalidRows.schema.exists { column =>
        column.name == "name" && column.dataType == StringType })

      val invalidIds = result.invalidRows.collect().map { _.getAs[String]("name") }

      assert(invalidIds.length == result.numInvalidRows)
      assert(invalidIds.count { _.startsWith("Product D") } == 2)
      assert(invalidIds.count { _.startsWith("Product C") } == 1)
      assert(invalidIds.count { _.startsWith("Product B") } == 1)

      assert(result.numInvalidRows == 5)
    }
  }
}
