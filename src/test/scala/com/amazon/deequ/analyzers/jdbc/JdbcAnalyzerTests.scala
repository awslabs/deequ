package com.amazon.deequ.analyzers.jdbc

import com.amazon.deequ.analyzers.jdbc
import com.amazon.deequ.analyzers.runners.NoSuchColumnException
import com.amazon.deequ.metrics.{Distribution, DoubleMetric, Entity, HistogramMetric}
import com.amazon.deequ.utils.AssertionUtils.TryUtils
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}

class JdbcAnalyzerTests
  extends WordSpec with Matchers with JdbcContextSpec with JdbcFixtureSupport {

  "Size analyzer" should {
    "compute correct metrics" in withJdbc { connection =>

      val tableMissing = getTableMissingWithSize(connection)
      val tableFull = getTableFullWithSize(connection)
      val tableFiltered = getTableFullWithSize(connection)
      val tableEmpty = getTableEmptyWithSize(connection)

      assert(JdbcSize().calculate(tableMissing._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableMissing._2)))
      assert(JdbcSize().calculate(tableFull._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableFull._2)))
      assert(JdbcSize(where = Some("item != 3")).calculate(tableFiltered._1) ==
        DoubleMetric(Entity.Dataset, "Size", "*", Success(3.0)))
      assert(JdbcSize().calculate(tableEmpty._1) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(tableEmpty._2)))

    }
  }

  "Distinctness analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)
      val tableFull = getTableFull(connection)

      assert(JdbcDistinctness("att1").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Distinctness", "att1", Success(1.0 / 6)))
      assert(JdbcDistinctness("att2").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Distinctness", "att2", Success(1.0 / 6)))

      assert(JdbcDistinctness("att1").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Distinctness", "att1", Success(0.5)))
      assert(JdbcDistinctness("att2").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Distinctness", "att2", Success(0.5)))

    }

    "error handling" should {
      "fail on empty column" in withJdbc { connection =>
        val table = getTableMissingColumn(connection)
        assert(JdbcDistinctness("att1").calculate(table).value.isFailure)
      }
      "fail on empty table" in withJdbc { connection =>
        val table = getTableEmpty(connection)
        assert(JdbcDistinctness("att1").calculate(table).value.isFailure)
      }
    }
  }

  "Histogram analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      // TODO
      val tableEmpty = getTableEmpty(connection)

      assert(JdbcHistogram("att1").calculate(tableEmpty) == HistogramMetric("att1",
        Success(Distribution(Map(), 0))))
    }
  }

  "Compliance analyzer" should {

    "compute correct metrics" in withJdbc { connection =>

      // TODO
    }

  }

  "Completeness analyzer" should {

    "compute correct metrics" in withJdbc { connection =>

      val tableMissing = getTableMissing(connection)
      val tableMissingColumn = getTableMissingColumn(connection)

      assert(JdbcCompleteness("someMissingColumn").preconditions.size == 2,
        "should check column name availability")
      assert(JdbcCompleteness("att1").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att1", Success(0.5)))
      assert(JdbcCompleteness("att2").calculate(tableMissing) == DoubleMetric(Entity.Column,
        "Completeness", "att2", Success(0.75)))
      assert(JdbcCompleteness("att1").calculate(tableMissingColumn) == DoubleMetric(Entity.Column,
        "Completeness", "att1", Success(0.0)))

    }

    "error handling" should {

      "fail on empty table" in withJdbc { connection =>
        val tableEmpty = getTableEmpty(connection)
        assert(JdbcCompleteness("att1").calculate(tableEmpty).value.isFailure)
      }

      "fail on wrong column input" in withJdbc { connection =>
        val tableMissing = getTableMissing(connection)

        JdbcCompleteness("someMissingColumn").calculate(tableMissing) match {
          case metric =>
            assert(metric.entity == Entity.Column)
            assert(metric.name == "Completeness")
            assert(metric.instance == "someMissingColumn")
            assert(metric.value.isFailure)
        }
      }

    }

    "work with filtering" in withJdbc { connection =>
      val dfMissing = getTableMissing(connection)

      val result = JdbcCompleteness("att1", Some("item IN ('1', '2')")).calculate(dfMissing)
      assert(result == DoubleMetric(Entity.Column, "Completeness", "att1", Success(1.0)))
    }

    "prevent sql injections" should {

      "prevent sql injection in table name for completeness" in withJdbc { connection =>
        val table = getTableMissing(connection)
        val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
        assert(JdbcCompleteness("att1").calculate(tableWithInjection).value.isFailure)
        assert(hasTable(connection, table.name))
      }
      "prevent sql injection in column name for completeness" in withJdbc { connection =>
        val table = getTableMissing(connection)
        val columnWithInjection = s"1 THEN 1 ELSE 0); DROP TABLE ${table.name};"
        assert(JdbcCompleteness(columnWithInjection).calculate(table).value.isFailure)
        assert(hasTable(connection, table.name))
      }
      "prevent sql injections in where clause for completeness" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        assert(JdbcCompleteness("att1",Some (s"';DROP TABLE ${table.name};--")).calculate(table).value.isFailure)
        assert(hasTable(connection, table.name))
      }
    }

  }

  "Uniqueness analyzers" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableMissing = getTableMissing(connection)
      val tableFull = getTableFull(connection)

      assert(JdbcUniqueness("att1").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Uniqueness", "att1", Success(0.0)))
      assert(JdbcUniqueness("att2").calculate(tableMissing) ==
        DoubleMetric(Entity.Column, "Uniqueness", "att2", Success(0.0)))


      assert(JdbcUniqueness("att1").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Uniqueness", "att1", Success(0.25)))
      assert(JdbcUniqueness("att2").calculate(tableFull) == DoubleMetric(Entity.Column,
        "Uniqueness", "att2", Success(0.25)))

    }

    "error handling" should {

      "fail on empty table" in withJdbc { connection =>
        val table = getTableEmpty(connection)
        assert(JdbcUniqueness("att1").calculate(table).value.isFailure)
      }
      "fail on empty column" in withJdbc { connection =>
        val table = getTableMissingColumn(connection)
        assert(JdbcUniqueness("att1").calculate(table).value.isFailure)
      }

    }
    "compute correct metrics on multi columns" in withJdbc { connection =>
      val dfFull = getTableWithUniqueColumns(connection)

      assert(JdbcUniqueness("uniqueValues").calculate(dfFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueValues", Success(1.0)))
      assert(JdbcUniqueness("uniqueWithNulls").calculate(dfFull) ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueWithNulls", Success(5 / 6.0)))
    }

    "fail on wrong column input" in withJdbc { connection =>
      val dfFull = getTableWithUniqueColumns(connection)

      JdbcUniqueness("nonExistingColumn").calculate(dfFull) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }

      JdbcUniqueness(Seq("nonExistingColumn", "uniqueValues")).calculate(dfFull) match {
        case metric =>
          assert(metric.entity == Entity.Mutlicolumn)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn,uniqueValues")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }
    }

    "prevent sql injections" should {

      "prevent sql injections in table name for uniqueness" in withJdbc { connection =>
        val table = getTableFull(connection)
        val tableWithInjection =
          Table(s"${table.name}) AS num_rows; DROP TABLE ${table.name};", connection)
        assert(JdbcUniqueness("att1").calculate(tableWithInjection).value.isFailure)
        assert(hasTable(connection, table.name))
      }
      "prevent sql injections in column name for uniqueness" in withJdbc { connection =>
        val table = getTableFull(connection)
        val columnWithInjection = s"nonExistingColumnName"
        assert(JdbcUniqueness(columnWithInjection).calculate(table).value.isFailure)
        assert(hasTable(connection, table.name))
      }

    }

    "UniqueValueRatio analyzer" should {

      "compute correct metrics" in withJdbc { connection =>
        val tableMissing = getTableMissing(connection)
        val tableFull = getTableFull(connection)

        assert(JdbcUniqueValueRatio("att1").calculate(tableMissing) ==
          DoubleMetric(Entity.Column, "UniqueValueRatio", "att1", Success(0.0)))
        assert(JdbcUniqueValueRatio("att2").calculate(tableMissing) ==
          DoubleMetric(Entity.Column, "UniqueValueRatio", "att2", Success(0.0)))


        assert(JdbcUniqueValueRatio("att1").calculate(tableFull) == DoubleMetric(Entity.Column,
          "UniqueValueRatio", "att1", Success(0.5)))
        assert(JdbcUniqueValueRatio("att2").calculate(tableFull) == DoubleMetric(Entity.Column,
          "UniqueValueRatio", "att2", Success(0.5)))

      }

      "error handling" should {

        "fail on empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(JdbcUniqueValueRatio("att1").calculate(table).value.isFailure)
        }
        "fail on empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(JdbcUniqueValueRatio("att1").calculate(table).value.isFailure)
        }

      }
    }
  }

  "Entropy analyzer" should {
    "compute correct metrics" in withJdbc { connection =>
      val tableFull = getTableFull(connection)
      val tableDistinct = getTableWithDistinctValues(connection)

      assert(JdbcEntropy("att1").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att1",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))
      assert(JdbcEntropy("att2").calculate(tableFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att2",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))

      assert(JdbcEntropy("att1").calculate(tableDistinct) ==
        DoubleMetric(Entity.Column, "Entropy", "att1",
          Success(
            -((1.0 / 3) * math.log(1.0 / 3)
              + (1.0 / 3) * math.log(1.0 / 3)
              + (1.0 / 6) * math.log(1.0 / 6)))))
      assert(JdbcEntropy("att2").calculate(tableDistinct) ==
        DoubleMetric(Entity.Column, "Entropy", "att2",
          Success(-(0.5 * math.log(0.5) + (1.0 / 6) * math.log(1.0 / 6)))))
    }

    "error handling" should {

      "fail on empty table" in withJdbc { connection =>
        val table = getTableEmpty(connection)
        assert(JdbcEntropy("att1").calculate(table).value.isFailure)
      }
      "fail on empty column" in withJdbc { connection =>
        val table = getTableMissingColumn(connection)
        assert(JdbcEntropy("att1").calculate(table).value.isFailure)
      }

    }
  }

  "Basic statistics" should {

    "Mean analyzer" should {
      "compute mean correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = JdbcMean("att1").calculate(table).value
        result shouldBe Success(3.5)
      }

      "compute mean correctly for numeric data with filtering" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = JdbcMean("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(3.0)
      }

      "error handling for mean" should {
        "fail to compute mean for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(JdbcMean("att1").calculate(table).value.isFailure)
        }
        "fail to compute mean for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(JdbcMean("att1").calculate(table).value.isFailure)
        }
        "fail to compute mean for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(JdbcMean("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for mean" should {
        "prevent sql injections in table name for mean" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(JdbcMean("att1").calculate(tableWithInjection).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for mean" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(JdbcMean(columnWithInjection).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for mean" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(JdbcMean("att1", Some(s"';DROP TABLE ${table.name};--")).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
      }
    }

    "Standard deviation analyzer" should {
      "compute standard deviation correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = JdbcStandardDeviation("att1").calculate(table).value
        result shouldBe Success(1.707825127659933)
      }

      "compute standard deviation correctly for numeric data with filtering" in withJdbc {
        connection =>
          val table = getTableWithNumericValues(connection)
          val result = JdbcStandardDeviation("att1", where =
            Some("item != '6'")).calculate(table).value
          result shouldBe Success(1.4142135623730951)
      }

      "error handling for standard deviation" should {
        "fail to compute standard deviation for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(JdbcStandardDeviation("att1").calculate(table).value.isFailure)
        }
        "fail to compute standard deviation for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(JdbcStandardDeviation("att1").calculate(table).value.isFailure)
        }
        "fail to compute standard deviaton for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(JdbcStandardDeviation("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for standard deviation" should {
        "prevent sql injections in table name for standard deviaton" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(JdbcStandardDeviation("att1").calculate(tableWithInjection).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for standard deviaton" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"nonExistingColumnName"
          assert(JdbcStandardDeviation(columnWithInjection).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for standard deviation" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(JdbcStandardDeviation("att1",Some (s"';DROP TABLE ${table.name};--")).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
      }
    }

    "Minimum analyzer" should {
      "compute minimum correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = JdbcMinimum("att1").calculate(table).value
        result shouldBe Success(1.0)
      }

      "compute minimum correctly for numeric data with filtering" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = JdbcMinimum("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(1.0)
      }

      "error handling for minimum" should {
        "fail to compute minimum for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(JdbcMinimum("att1").calculate(table).value.isFailure)
        }
        "fail to compute minimum for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(JdbcMinimum("att1").calculate(table).value.isFailure)
        }
        "fail to compute minimum for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(JdbcMinimum("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for minimum" should {
        "prevent sql injections in table name for minimum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(JdbcMinimum("att1").calculate(tableWithInjection).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for minimum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(JdbcMinimum(columnWithInjection).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for minimum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(JdbcMinimum("att1",Some (s"';DROP TABLE ${table.name};--")).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
      }
    }

    "Maximum analyzer" should {
      "compute maximum correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = JdbcMaximum("att1").calculate(table).value
        result shouldBe Success(6.0)
      }

      "compute maximum correctly for numeric data with filtering" in
        withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val result = JdbcMaximum("att1", where = Some("item != '6'")).calculate(table).value
          result shouldBe Success(5.0)
        }

      "error handling for maximum" should {
        "fail to compute maximum for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(JdbcMaximum("att1").calculate(table).value.isFailure)
        }
        "fail to compute maximum for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(JdbcMaximum("att1").calculate(table).value.isFailure)
        }
        "fail to compute maximum for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(JdbcMaximum("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for maximum" should {
        "prevent sql injections in table name for maximum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(JdbcMaximum("att1").calculate(tableWithInjection).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for maximum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(JdbcMaximum(columnWithInjection).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for maximum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(JdbcMaximum("att1",Some (s"';DROP TABLE ${table.name};--")).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
      }
    }

    "Sum analyzer" should {
      "compute sum correctly for numeric data" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        JdbcSum("att1").calculate(table).value shouldBe Success(21)
      }

      "compute sum correctly for numeric data with filtering" in withJdbc { connection =>
        val table = getTableWithNumericValues(connection)
        val result = JdbcSum("att1", where = Some("item != '6'")).calculate(table).value
        result shouldBe Success(15.0)
      }

      "should work correctly on decimal columns" in withJdbc { connection =>
        /*
        val schema =
          StructType(StructField(name = "num", dataType = DecimalType.SYSTEM_DEFAULT) :: Nil)

        val rows = session.sparkContext.parallelize(Seq(
          Row(BigDecimal(123.45)),
          Row(BigDecimal(99)),
          Row(BigDecimal(678))))

        val data = session.createDataFrame(rows, schema)

        val result = Minimum("num").calculate(data)

        assert(result.value.isSuccess)
        assert(result.value.get == 99.0)
        */
      }

      "error handling for sum" should {
        "fail to compute sum for non numeric type" in withJdbc { connection =>
          val table = getTableFull(connection)
          assert(JdbcSum("att1").calculate(table).value.isFailure)
        }
        "fail to compute sum for empty table" in withJdbc { connection =>
          val table = getTableEmpty(connection)
          assert(JdbcSum("att1").calculate(table).value.isFailure)
        }
        "fail to compute sum for empty column" in withJdbc { connection =>
          val table = getTableMissingColumn(connection)
          assert(JdbcSum("att1").calculate(table).value.isFailure)
        }
      }

      "prevent sql injections for sum" should {
        "prevent sql injections in table name for sum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(JdbcSum("att1").calculate(tableWithInjection).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for sum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(JdbcSum(columnWithInjection).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in where clause for sum" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          assert(JdbcSum("att1",Some (s"';DROP TABLE ${table.name};--")).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
      }
    }


  }

  "Count distinct analyzers" should {
    "compute approximate distinct count for numeric data" in withJdbc { connection =>
      /*
      val table = getTableWithUniqueColumns(connection)
      val result = JdbcApproxCountDistinct("uniqueWithNulls").calculate(table).value

      result shouldBe Success(5.0)
      */
    }

    "compute approximate distinct count for numeric data with filtering" in
      withJdbc { connection =>

        /*
        val table = getTableWithUniqueColumns(connection)
        val result = JdbcApproxCountDistinct("uniqueWithNulls", where = Some("unique < 4"))
          .calculate(table).value
        result shouldBe Success(2.0)
        */
      }

    "Count distinct analyzer" should {
      "compute correct metrics for distinct count" should {
        "compute exact distinct count of elements for numeric data" in withJdbc {
          connection =>
            val table = getTableWithUniqueColumns(connection)
            val result = JdbcCountDistinct("uniqueWithNulls").calculate(table).value
            result shouldBe Success(5.0)
        }
        "compute exact distinct count of elements for empty table" in withJdbc {
          connection =>
            val table = getTableEmpty(connection)
            val result = JdbcCountDistinct("att1").calculate(table).value
            result shouldBe Success(0.0)
        }
        "compute exact distinct count of elements for empty column" in withJdbc {
          connection =>
            val table = getTableMissingColumn(connection)
            val result = JdbcCountDistinct("att1").calculate(table).value
            result shouldBe Success(0.0)
        }
      }

      "prevent sql injection for distinct count" should {
        "prevent sql injections in table name for count distinct" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val tableWithInjection = Table(s"${table.name}; DROP TABLE ${table.name};", connection)
          assert(JdbcCountDistinct("att1").calculate(tableWithInjection).value.isFailure)
          assert(hasTable(connection, table.name))
        }
        "prevent sql injections in column name for count distinct" in withJdbc { connection =>
          val table = getTableWithNumericValues(connection)
          val columnWithInjection = s"1); DROP TABLE ${table.name};"
          assert(JdbcCountDistinct(columnWithInjection).calculate(table).value.isFailure)
          assert(hasTable(connection, table.name))
        }
      }
    }
  }
}

object JdbcAnalyzerTests {
  val expectedPreconditionViolation: String =
    "computation was unexpectedly successful, should have failed due to violated precondition"
}
