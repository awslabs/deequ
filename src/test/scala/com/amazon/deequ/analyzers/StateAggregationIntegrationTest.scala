package com.amazon.deequ.analyzers

import com.amazon.deequ.{SparkContextSpec, VerificationSuite}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{Matchers, WordSpec}
import org.apache.spark.sql.functions.expr

class StateAggregationIntegrationTest extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "State aggregation" should {
    "work correctly over data partitions" in withSparkSession { session =>

      val schema = StructType(
        StructField("asin", StringType, nullable = false) ::
        StructField("origin", StringType, nullable = true) ::
        StructField("sales", IntegerType, nullable = false) ::
        StructField("marketplace", StringType, nullable = false) :: Nil)

      val rowData = Seq(
        Row("asin1", "US", 100, "EU"),
        Row("asin1", "US", 1000, "NA"),
        Row("asin1", "US", 20, "IN"),

        Row("asin2", "DE", 20, "EU"),
        Row("asin2", "DE", 333, "NA"),

        Row("asin3", null, 12, "NA"),
        Row("asin4", null, 45, "NA"),
        Row("asin5", null, 123, "NA"))

      val data = session.createDataFrame(session.sparkContext.parallelize(rowData, 2), schema)

      val partitionNA = data.where(expr("marketplace = 'NA'"))
      val partitionEU = data.where(expr("marketplace = 'EU'"))
      val partitionIN = data.where(expr("marketplace = 'IN'"))

      val completeness = Completeness("origin")

      val completenessStateNA = completeness.computeStateFrom(partitionNA)
      val completenessStateEU = completeness.computeStateFrom(partitionEU)
      val completenessStateIN = completeness.computeStateFrom(partitionIN)

      val completenessState = Analyzers.merge(completenessStateNA, completenessStateEU,
        completenessStateIN)

      val completenessNA = completeness.computeMetricFrom(completenessStateNA)
      val completenessEU = completeness.computeMetricFrom(completenessStateEU)
      val completenessIN = completeness.computeMetricFrom(completenessStateIN)
      val overallCompleteness = completeness.computeMetricFrom(completenessState)

      println(s"Completeness of origin in NA partition: ${completenessNA.value}")
      println(s"Completeness of origin in EU partition: ${completenessEU.value}")
      println(s"Completeness of origin in IN partition: ${completenessIN.value}")
      println(s"Completeness of origin overall: ${overallCompleteness.value}")

      assert(overallCompleteness == completeness.calculate(data))

      println()

      val standardDeviation = StandardDeviation("sales")

      val standardDeviationStateNA = standardDeviation.computeStateFrom(partitionNA)
      val standardDeviationStateEU = standardDeviation.computeStateFrom(partitionEU)
      val standardDeviationStateIN = standardDeviation.computeStateFrom(partitionIN)

      val standardDeviationState = Analyzers.merge(standardDeviationStateNA,
        standardDeviationStateEU, standardDeviationStateIN)

      val standardDeviationNA =
        standardDeviation.computeMetricFrom(standardDeviationStateNA)
      val standardDeviationEU =
        standardDeviation.computeMetricFrom(standardDeviationStateEU)
      val standardDeviationIN =
        standardDeviation.computeMetricFrom(standardDeviationStateIN)
      val overallStandardDeviation =
        standardDeviation.computeMetricFrom(standardDeviationState)

      println(s"Standard deviation of sales in NA partition: ${standardDeviationNA.value}")
      println(s"Standard deviation of sales in EU partition: ${standardDeviationEU.value}")
      println(s"Standard deviation of sales in IN partition: ${standardDeviationIN.value}")
      println(s"Standard deviation of sales overall: ${overallStandardDeviation.value}")

      assert(overallStandardDeviation == standardDeviation.calculate(data))

      println()

      val distinctness = Distinctness("asin")

      val distinctnessStateNA = distinctness.computeStateFrom(partitionNA)
      val distinctnessStateEU = distinctness.computeStateFrom(partitionEU)
      val distinctnessStateIN = distinctness.computeStateFrom(partitionIN)

      val distinctnessState = Analyzers.merge(distinctnessStateNA, distinctnessStateEU,
        distinctnessStateIN)

      val distinctnessNA = distinctness.computeMetricFrom(distinctnessStateNA)
      val distinctnessEU = distinctness.computeMetricFrom(distinctnessStateEU)
      val distinctnessIN = distinctness.computeMetricFrom(distinctnessStateIN)
      val overallDistinctness = distinctness.computeMetricFrom(distinctnessState)

      println(s"Distinctness of asin in NA partition: ${distinctnessNA.value}")
      println(s"Distinctness of asin in EU partition: ${distinctnessEU.value}")
      println(s"Distinctness of asin in IN partition: ${distinctnessIN.value}")
      println(s"Distinctness of asin overall: ${overallDistinctness.value}")

      assert(overallDistinctness == distinctness.calculate(data))
    }

    "work correctly via AnalysisRunner" in withSparkSession { session =>

      val schema = StructType(
        StructField("asin", StringType, nullable = false) ::
        StructField("origin", StringType, nullable = true) ::
        StructField("sales", IntegerType, nullable = false) ::
        StructField("marketplace", StringType, nullable = false) :: Nil)

      val rowData = Seq(
        Row("asin1", "US", 100, "EU"),
        Row("asin1", "US", 1000, "NA"),
        Row("asin1", "US", 20, "IN"),

        Row("asin2", "DE", 20, "EU"),
        Row("asin2", "DE", 333, "NA"),

        Row("asin3", null, 12, "NA"),
        Row("asin4", null, 45, "NA"),
        Row("asin5", null, 123, "NA"))

      val data = session.createDataFrame(session.sparkContext.parallelize(rowData, 2), schema)

      val partitionNA = data.where(expr("marketplace = 'NA'"))
      val partitionEU = data.where(expr("marketplace = 'EU'"))
      val partitionIN = data.where(expr("marketplace = 'IN'"))

      val greaterThanHalf: Double => Boolean = { _ > 0.5 }

      val check = Check(CheckLevel.Error, "check")
        .isComplete("asin")
        .isNonNegative("sales")
        .isContainedIn("marketplace", Array("EU", "NA", "IN"))
        .hasApproxCountDistinct("asin", _ < 10)
        .hasUniqueness(Seq("asin"), greaterThanHalf)
        .hasUniqueValueRatio(Seq("asin"), greaterThanHalf)

      val analyzersFromChecks = Seq(check).flatMap { _.requiredAnalyzers() }

      val analysis = Analysis(analyzersFromChecks)

      val statesNA = InMemoryStateProvider()
      val statesEU = InMemoryStateProvider()
      val statesIN = InMemoryStateProvider()

      // TODO: Since the new fluent API does not support saving of states yet, the deprecated
      // TODO: method is still needed here
      AnalysisRunner.run(partitionNA, analysis, saveStatesWith = Some(statesNA))
      AnalysisRunner.run(partitionEU, analysis, saveStatesWith = Some(statesEU))
      AnalysisRunner.run(partitionIN, analysis, saveStatesWith = Some(statesIN))

      val resultsFromAggregation = AnalysisRunner
        .runOnAggregatedStates(schema, analysis, Seq(statesNA, statesEU, statesIN))

      val results = AnalysisRunner.onData(data).addAnalyzers(analyzersFromChecks).run()

      assert(resultsFromAggregation == results)
    }

    "work correctly via VerificationSuite" in withSparkSession { session =>

      val schema = StructType(
        StructField("asin", StringType, nullable = false) ::
        StructField("origin", StringType, nullable = true) ::
        StructField("sales", IntegerType, nullable = false) ::
        StructField("marketplace", StringType, nullable = false) :: Nil)

      val rowData = Seq(
        Row("asin1", "US", 100, "EU"),
        Row("asin1", "US", 1000, "NA"),
        Row("asin1", "US", 20, "IN"),

        Row("asin2", "DE", 20, "EU"),
        Row("asin2", "DE", 333, "NA"),

        Row("asin3", null, 12, "NA"),
        Row("asin4", null, 45, "NA"),
        Row("asin5", null, 123, "NA"))

      val data = session.createDataFrame(session.sparkContext.parallelize(rowData, 2), schema)

      val partitionNA = data.where(expr("marketplace = 'NA'"))
      val partitionEU = data.where(expr("marketplace = 'EU'"))
      val partitionIN = data.where(expr("marketplace = 'IN'"))

      val greaterThanHalf: Double => Boolean = { _ > 0.5 }

      val check = Check(CheckLevel.Error, "check")
        .isComplete("asin")
        .isNonNegative("sales")
        .isContainedIn("marketplace", Array("EU", "NA", "IN"))
        .hasApproxCountDistinct("asin", _ < 10)
        .hasUniqueness(Seq("asin"), greaterThanHalf)
        .hasUniqueValueRatio(Seq("asin"), greaterThanHalf)

      val analyzersFromChecks = Seq(check).flatMap { _.requiredAnalyzers() }

      val analysis = Analysis(analyzersFromChecks)

      val statesNA = InMemoryStateProvider()
      val statesEU = InMemoryStateProvider()
      val statesIN = InMemoryStateProvider()

      AnalysisRunner.run(partitionNA, analysis, saveStatesWith = Some(statesNA))
      AnalysisRunner.run(partitionEU, analysis, saveStatesWith = Some(statesEU))
      AnalysisRunner.run(partitionIN, analysis, saveStatesWith = Some(statesIN))

      val resultsDirect = VerificationSuite().onData(data).addCheck(check).run()
      val resultsFromStates = VerificationSuite.runOnAggregatedStates(schema, Seq(check),
        Seq(statesNA, statesEU, statesIN))

      assert(resultsFromStates == resultsDirect)
    }

  }

}
