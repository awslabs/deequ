package com.amazon.deequ.analyzers.jdbc

import java.sql.Connection

import com.amazon.deequ.analyzers.State
import com.amazon.deequ.metrics.Metric
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

class JdbcStateAggregationTests extends WordSpec with Matchers with JdbcContextSpec
  with JdbcFixtureSupport {

  "State aggregation outside" should {

    "give correct results" in withJdbc { connection =>

      correctlyAggregatesStates(connection, JdbcSize())
      correctlyAggregatesStates(connection, JdbcMaximum("marketplace_id"))
      correctlyAggregatesStates(connection, JdbcMinimum("marketplace_id"))
      correctlyAggregatesStates(connection, JdbcMean("marketplace_id"))
      correctlyAggregatesStates(connection, JdbcStandardDeviation("marketplace_id"))
      correctlyAggregatesStates(connection, JdbcUniqueness("attribute" :: "value" :: Nil))
      correctlyAggregatesStates(connection, JdbcDistinctness("attribute" :: Nil))
      correctlyAggregatesStates(connection, JdbcCountDistinct("value" :: Nil))
      correctlyAggregatesStates(connection, JdbcUniqueValueRatio("value"))
      correctlyAggregatesStates(connection, JdbcCompleteness("attribute"))
      correctlyAggregatesStates(connection,
        JdbcCompliance("attribute", "attribute like '%facets%'"))
      correctlyAggregatesStates(connection, JdbcCorrelation("numbersA", "numbersB"))
      correctlyAggregatesStates(connection, JdbcEntropy("attribute"))
      correctlyAggregatesStates(connection, JdbcHistogram("value"))
    }
  }

  def correctlyAggregatesStates[S <: State[S]](
                                                connection: Connection,
                                                analyzer: JdbcAnalyzer[S, Metric[_]])
  : Unit = {

    val dataA = initialData(connection)
    val dataB = deltaData(connection)
    val dataAB = completeData(connection)

    val stateA = analyzer.computeStateFrom(dataA)
    val stateB = analyzer.computeStateFrom(dataB)

    val metricFromCalculate = analyzer.calculate(dataAB)
    val mergedState = JdbcAnalyzers.merge(stateA, stateB)

    val metricFromAggregation = analyzer.computeMetricFrom(mergedState)

    assert(metricFromAggregation == metricFromCalculate)
  }

  def initialData(conn: Connection): Table = {
    val columns = mutable.LinkedHashMap[String, String](
      "marketplace_id" -> "INTEGER", "item" -> "TEXT", "attribute" -> "TEXT", "value" -> "TEXT",
    "numbersA" -> "INTEGER", "numbersB" -> "INTEGER")

    val data = Seq(
      Seq(1, "B00BJXTG66", "2nd story llc-0-$ims_facets-0-", "extended", 3, 12),
      Seq(1, "B00BJXTG66", "2nd story llc-0-value", "Intimate Organics", 4, 951510),
      Seq(1, "B00DLT13JY", "Binding-0-$ims_facets-0-", "extended", 5, 60),
      Seq(1, "B00ICANXP4", "Binding-0-$ims_facets-0-", "extended", 6, 655),
      Seq(1, "B00MG1DSWI", "Binding-0-$ims_facets-0-", "extended", 7, 45),
      Seq(1, "B00DLT13JY", "Binding-0-value", "consumer_electronics", 49, 2012),
      Seq(1, "B00ICANXP4", "Binding-0-value", "pc", 50, 68),
      Seq(1, "B00MG1DSWI", "Binding-0-value", "toy", 51, 90),
      Seq(1, "B0012P3IYC", "CATEGORY-0-$ims_facets-0-", "extended", 52, 1),
      Seq(1, "B001FFEJY2", "CATEGORY-0-$ims_facets-0-", "extended", 53, 78),
      Seq(1, "B001GF63ZO", "CATEGORY-0-$ims_facets-0-", "extended", 54, 33),
      Seq(1, "B001RKKJRQ", "CATEGORY-0-$ims_facets-0-", "extended", 27, 90),
      Seq(1, "B001RLFZTW", "CATEGORY-0-$ims_facets-0-", "extended", 28, 2),
      Seq(1, "B001RMNV6A", "CATEGORY-0-$ims_facets-0-", "extended", 29, 57),
      Seq(1, "B001RPWK9G", "CATEGORY-0-$ims_facets-0-", "extended", 1029, 39),
      Seq(1, "B001RQ37ME", "CATEGORY-0-$ims_facets-0-", "extended", 1030, 80),
      Seq(1, "B001RQJHVE", "CATEGORY-0-$ims_facets-0-", "extended", 13, 12),
      Seq(1, "B001RRX642", "CATEGORY-0-$ims_facets-0-", "extended", 17, 0),
      Seq(1, "B001RS3C2C", "CATEGORY-0-$ims_facets-0-", "extended", 55, 0),
      Seq(1, "B001RTDRO4", "CATEGORY-0-$ims_facets-0-", "extended", 83, 0))

    fillTableWithData(conn: Connection, "initialData", columns, data)
  }

  def deltaData(conn: Connection): Table = {
    val columns = mutable.LinkedHashMap[String, String](
      "marketplace_id" -> "INTEGER", "item" -> "TEXT", "attribute" -> "TEXT", "value" -> "TEXT",
      "numbersA" -> "INTEGER", "numbersB" -> "INTEGER")
    val data = Seq(
      Seq(1, "B008FZTBAW", "BroadITKitem_type_keyword-0-", "jewelry-products", 100, 7),
      Seq(1, "B00BUU5R02", "BroadITKitem_type_keyword-0-", "kitchen-products", 99, 8),
      Seq(1, "B0054UJNJK", "BroadITKitem_type_keyword-0-", "lighting-products", 98, 9),
      Seq(1, "B00575Q69M", "BroadITKitem_type_keyword-0-", "lighting-products", 97, 10),
      Seq(1, "B005F2OSTC", "BroadITKitem_type_keyword-0-", "lighting-products", 96, 11),
      Seq(1, "B00BQNCQWU", "BroadITKitem_type_keyword-0-", "lighting-products", 95, 12),
      Seq(1, "B00BQND3WC", "BroadITKitem_type_keyword-0-", "lighting-products", 94, 13),
      Seq(1, "B00C1CU3PC", "BroadITKitem_type_keyword-0-", "lighting-products", 93, 14),
      Seq(1, "B00C1CYE66", "BroadITKitem_type_keyword-0-", "lighting-products", 92, 15),
      Seq(1, "B00C1CYIKS", "BroadITKitem_type_keyword-0-", "lighting-products", 91, 18),
      Seq(1, "B00C1CZ2NK", "BroadITKitem_type_keyword-0-", "lighting-products", 90, 25),
      Seq(1, "B00C1D26SI", "BroadITKitem_type_keyword-0-", "lighting-products", 89, 27),
      Seq(1, "B00C1D2HQ4", "BroadITKitem_type_keyword-0-", "lighting-products", 88, 28),
      Seq(1, "B00C1D554A", "BroadITKitem_type_keyword-0-", "lighting-products", 87, 29),
      Seq(1, "B00C1D5I0Q", "BroadITKitem_type_keyword-0-", "lighting-products", 86, 30),
      Seq(1, "B00C1D5LU8", "BroadITKitem_type_keyword-0-", "lighting-products", 85, 60),
      Seq(1, "B00C1D927G", "BroadITKitem_type_keyword-0-", "lighting-products", 84, 62),
      Seq(1, "B00C1DAXMO", "BroadITKitem_type_keyword-0-", "lighting-products", 83, 70),
      Seq(1, "B00C1DDNC6", "BroadITKitem_type_keyword-0-", "lighting-products", 82, 71),
      Seq(1, "B00CF0URZ6", "BroadITKitem_type_keyword-0-", "lighting-products", 81, 72))

    fillTableWithData(conn: Connection, "deltaData", columns, data)
  }

  def completeData(conn: Connection): Table = {
    val columns = mutable.LinkedHashMap[String, String](
      "marketplace_id" -> "INTEGER", "item" -> "TEXT", "attribute" -> "TEXT", "value" -> "TEXT",
      "numbersA" -> "INTEGER", "numbersB" -> "INTEGER")

    val data = Seq(
      Seq(1, "B00BJXTG66", "2nd story llc-0-$ims_facets-0-", "extended", 3, 12),
      Seq(1, "B00BJXTG66", "2nd story llc-0-value", "Intimate Organics", 4, 951510),
      Seq(1, "B00DLT13JY", "Binding-0-$ims_facets-0-", "extended", 5, 60),
      Seq(1, "B00ICANXP4", "Binding-0-$ims_facets-0-", "extended", 6, 655),
      Seq(1, "B00MG1DSWI", "Binding-0-$ims_facets-0-", "extended", 7, 45),
      Seq(1, "B00DLT13JY", "Binding-0-value", "consumer_electronics", 49, 2012),
      Seq(1, "B00ICANXP4", "Binding-0-value", "pc", 50, 68),
      Seq(1, "B00MG1DSWI", "Binding-0-value", "toy", 51, 90),
      Seq(1, "B0012P3IYC", "CATEGORY-0-$ims_facets-0-", "extended", 52, 1),
      Seq(1, "B001FFEJY2", "CATEGORY-0-$ims_facets-0-", "extended", 53, 78),
      Seq(1, "B001GF63ZO", "CATEGORY-0-$ims_facets-0-", "extended", 54, 33),
      Seq(1, "B001RKKJRQ", "CATEGORY-0-$ims_facets-0-", "extended", 27, 90),
      Seq(1, "B001RLFZTW", "CATEGORY-0-$ims_facets-0-", "extended", 28, 2),
      Seq(1, "B001RMNV6A", "CATEGORY-0-$ims_facets-0-", "extended", 29, 57),
      Seq(1, "B001RPWK9G", "CATEGORY-0-$ims_facets-0-", "extended", 1029, 39),
      Seq(1, "B001RQ37ME", "CATEGORY-0-$ims_facets-0-", "extended", 1030, 80),
      Seq(1, "B001RQJHVE", "CATEGORY-0-$ims_facets-0-", "extended", 13, 12),
      Seq(1, "B001RRX642", "CATEGORY-0-$ims_facets-0-", "extended", 17, 0),
      Seq(1, "B001RS3C2C", "CATEGORY-0-$ims_facets-0-", "extended", 55, 0),
      Seq(1, "B001RTDRO4", "CATEGORY-0-$ims_facets-0-", "extended", 83, 0),
      Seq(1, "B008FZTBAW", "BroadITKitem_type_keyword-0-", "jewelry-products", 100, 7),
      Seq(1, "B00BUU5R02", "BroadITKitem_type_keyword-0-", "kitchen-products", 99, 8),
      Seq(1, "B0054UJNJK", "BroadITKitem_type_keyword-0-", "lighting-products", 98, 9),
      Seq(1, "B00575Q69M", "BroadITKitem_type_keyword-0-", "lighting-products", 97, 10),
      Seq(1, "B005F2OSTC", "BroadITKitem_type_keyword-0-", "lighting-products", 96, 11),
      Seq(1, "B00BQNCQWU", "BroadITKitem_type_keyword-0-", "lighting-products", 95, 12),
      Seq(1, "B00BQND3WC", "BroadITKitem_type_keyword-0-", "lighting-products", 94, 13),
      Seq(1, "B00C1CU3PC", "BroadITKitem_type_keyword-0-", "lighting-products", 93, 14),
      Seq(1, "B00C1CYE66", "BroadITKitem_type_keyword-0-", "lighting-products", 92, 15),
      Seq(1, "B00C1CYIKS", "BroadITKitem_type_keyword-0-", "lighting-products", 91, 18),
      Seq(1, "B00C1CZ2NK", "BroadITKitem_type_keyword-0-", "lighting-products", 90, 25),
      Seq(1, "B00C1D26SI", "BroadITKitem_type_keyword-0-", "lighting-products", 89, 27),
      Seq(1, "B00C1D2HQ4", "BroadITKitem_type_keyword-0-", "lighting-products", 88, 28),
      Seq(1, "B00C1D554A", "BroadITKitem_type_keyword-0-", "lighting-products", 87, 29),
      Seq(1, "B00C1D5I0Q", "BroadITKitem_type_keyword-0-", "lighting-products", 86, 30),
      Seq(1, "B00C1D5LU8", "BroadITKitem_type_keyword-0-", "lighting-products", 85, 60),
      Seq(1, "B00C1D927G", "BroadITKitem_type_keyword-0-", "lighting-products", 84, 62),
      Seq(1, "B00C1DAXMO", "BroadITKitem_type_keyword-0-", "lighting-products", 83, 70),
      Seq(1, "B00C1DDNC6", "BroadITKitem_type_keyword-0-", "lighting-products", 82, 71),
      Seq(1, "B00CF0URZ6", "BroadITKitem_type_keyword-0-", "lighting-products", 81, 72))

    fillTableWithData(conn: Connection, "completeData", columns, data)
  }
}
