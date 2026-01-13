/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 */

package com.amazon.deequ.connect

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.analyzers.{Analyzer, DataTypeInstances, KLLParameters}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.checks.Check
import com.amazon.deequ.connect.proto._
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.profiles._
import com.amazon.deequ.suggestions.{ConstraintSuggestion, ConstraintSuggestionRunner, Rules}
import com.google.gson.{Gson, JsonObject}
import com.google.protobuf.{Any => ProtobufAny}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.JavaConverters._

/**
 * Spark Connect RelationPlugin for Deequ.
 *
 * This plugin handles custom Deequ relation types sent from Python clients,
 * executes the corresponding Deequ operations, and returns results as DataFrames.
 *
 * Register this plugin with:
 * --conf spark.connect.extensions.relation.classes=com.amazon.deequ.connect.DeequRelationPlugin
 */
class DeequRelationPlugin extends RelationPlugin {

  // The transform method receives protobuf Any from Spark Connect
  // Scala compiler sees com.google.protobuf.Any in the interface signature
  override def transform(
      relation: ProtobufAny,
      planner: SparkConnectPlanner): Option[LogicalPlan] = {

    // Debug: Log what we're receiving
    println(s"[DeequPlugin] Received relation with type_url: ${relation.getTypeUrl}")
    println(s"[DeequPlugin] Expected type_url for verification: ${DeequVerificationRelation.getDescriptor.getFullName}")
    println(s"[DeequPlugin] is(DeequVerificationRelation): ${relation.is(classOf[DeequVerificationRelation])}")

    // Handle verification requests
    if (relation.is(classOf[DeequVerificationRelation])) {
      println("[DeequPlugin] Handling verification request")
      val req = relation.unpack(classOf[DeequVerificationRelation])
      return Some(handleVerification(req, planner))
    }

    // Handle analysis requests
    if (relation.is(classOf[DeequAnalysisRelation])) {
      println("[DeequPlugin] Handling analysis request")
      val req = relation.unpack(classOf[DeequAnalysisRelation])
      return Some(handleAnalysis(req, planner))
    }

    // Handle column profiler requests
    if (relation.is(classOf[DeequColumnProfilerRelation])) {
      println("[DeequPlugin] Handling column profiler request")
      val req = relation.unpack(classOf[DeequColumnProfilerRelation])
      return Some(handleColumnProfiler(req, planner))
    }

    // Handle constraint suggestion requests
    if (relation.is(classOf[DeequConstraintSuggestionRelation])) {
      println("[DeequPlugin] Handling constraint suggestion request")
      val req = relation.unpack(classOf[DeequConstraintSuggestionRelation])
      return Some(handleConstraintSuggestion(req, planner))
    }

    // Not our message type
    println(s"[DeequPlugin] Unknown message type, returning None")
    None
  }

  /**
   * Handle a verification request - run checks and return results as DataFrame.
   */
  private def handleVerification(
      req: DeequVerificationRelation,
      planner: SparkConnectPlanner): LogicalPlan = {

    val spark = planner.sessionHolder.session

    // Get the input DataFrame from the serialized relation
    val inputDf = deserializeInputRelation(req.getInputRelation, planner)

    // Build Check objects from protobuf messages
    val checks = req.getChecksList.asScala.map(CheckBuilder.build).toSeq

    // Build required analyzers
    val analyzers = req.getRequiredAnalyzersList.asScala.map(AnalyzerBuilder.build).toSeq

    // Run verification
    var suite = VerificationSuite().onData(inputDf)
    checks.foreach(check => suite = suite.addCheck(check))
    analyzers.foreach(analyzer => suite = suite.addRequiredAnalyzer(analyzer))

    val result = suite.run()

    // Convert result to DataFrame
    verificationResultToDataFrame(spark, result).queryExecution.logical
  }

  /**
   * Handle an analysis request - run analyzers and return metrics as DataFrame.
   */
  private def handleAnalysis(
      req: DeequAnalysisRelation,
      planner: SparkConnectPlanner): LogicalPlan = {

    val spark = planner.sessionHolder.session

    // Get the input DataFrame
    val inputDf = deserializeInputRelation(req.getInputRelation, planner)

    // Build analyzers from protobuf messages
    val analyzers = req.getAnalyzersList.asScala.map(AnalyzerBuilder.build).toSeq

    // Run analysis
    val analysisResult = AnalysisRunner.onData(inputDf).addAnalyzers(analyzers).run()

    // Convert result to DataFrame
    analyzerContextToDataFrame(spark, analysisResult).queryExecution.logical
  }

  /**
   * Deserialize the input relation bytes to a DataFrame.
   */
  private def deserializeInputRelation(
      inputRelationBytes: com.google.protobuf.ByteString,
      planner: SparkConnectPlanner): DataFrame = {

    // Parse the bytes as a Spark Connect Relation message
    val relation = org.apache.spark.connect.proto.Relation.parseFrom(inputRelationBytes.toByteArray)

    // Use the planner to transform the relation to a logical plan
    val logicalPlan = planner.transformRelation(relation)

    // Create DataFrame from logical plan
    val spark = planner.sessionHolder.session
    val qe = spark.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new DataFrame(spark, logicalPlan, ExpressionEncoder(qe.analyzed.schema))
  }

  /**
   * Convert VerificationResult to a DataFrame with check results.
   */
  private def verificationResultToDataFrame(
      spark: SparkSession,
      result: VerificationResult): DataFrame = {

    import spark.implicits._

    // Get check results as rows
    val checkResults = result.checkResults.flatMap { case (check, checkResult) =>
      checkResult.constraintResults.map { constraintResult =>
        (
          check.description,
          check.level.toString,
          checkResult.status.toString,
          constraintResult.constraint.toString,
          constraintResult.status.toString,
          constraintResult.message.getOrElse(""),
          constraintResult.metric.map(_.value.toString).getOrElse("")
        )
      }
    }.toSeq

    checkResults.toDF(
      "check",
      "check_level",
      "check_status",
      "constraint",
      "constraint_status",
      "constraint_message",
      "metric_value"
    )
  }

  /**
   * Convert AnalyzerContext to a DataFrame with metrics.
   */
  private def analyzerContextToDataFrame(
      spark: SparkSession,
      context: AnalyzerContext): DataFrame = {

    import spark.implicits._
    import com.amazon.deequ.metrics.DoubleMetric

    // Get metrics as rows with proper Double type for value
    // Filter to only DoubleMetric types and extract the Double value
    val metrics = context.metricMap.toSeq.collect {
      case (analyzer, metric: DoubleMetric) =>
        val value: Double = metric.value.getOrElse(Double.NaN)
        (
          analyzer.toString,
          metric.entity.toString,
          metric.instance,
          metric.name,
          value
        )
    }

    metrics.toDF(
      "analyzer",
      "entity",
      "instance",
      "name",
      "value"
    )
  }

  /**
   * Handle a column profiler request - profile columns and return results as DataFrame.
   */
  private def handleColumnProfiler(
      req: DeequColumnProfilerRelation,
      planner: SparkConnectPlanner): LogicalPlan = {

    val spark = planner.sessionHolder.session
    val inputDf = deserializeInputRelation(req.getInputRelation, planner)

    // Build profiler runner
    var profilerRunner = ColumnProfilerRunner().onData(inputDf)

    // Restrict to columns if specified
    val restrictToColumns = req.getRestrictToColumnsList.asScala.toSeq
    if (restrictToColumns.nonEmpty) {
      profilerRunner = profilerRunner.restrictToColumns(restrictToColumns)
    }

    // Set histogram threshold
    if (req.getLowCardinalityHistogramThreshold > 0) {
      profilerRunner = profilerRunner
        .withLowCardinalityHistogramThreshold(req.getLowCardinalityHistogramThreshold)
    }

    // Enable KLL profiling if requested
    if (req.getEnableKllProfiling) {
      profilerRunner = profilerRunner.withKLLProfiling()

      // Set KLL parameters if provided
      if (req.hasKllParameters) {
        val kllParams = req.getKllParameters
        profilerRunner = profilerRunner.setKLLParameters(Some(KLLParameters(
          kllParams.getSketchSize,
          kllParams.getShrinkingFactor,
          kllParams.getNumberOfBuckets
        )))
      }
    }

    // Set predefined types if provided
    val predefinedTypes = req.getPredefinedTypesMap.asScala.toMap
    if (predefinedTypes.nonEmpty) {
      val typeMap = predefinedTypes.map { case (col, typeName) =>
        col -> parseDataType(typeName)
      }
      profilerRunner = profilerRunner.setPredefinedTypes(typeMap)
    }

    // Run profiler
    val profiles = profilerRunner.run()

    // Convert to DataFrame
    columnProfilesToDataFrame(spark, profiles).queryExecution.logical
  }

  /**
   * Handle a constraint suggestion request - generate suggestions and return as DataFrame.
   */
  private def handleConstraintSuggestion(
      req: DeequConstraintSuggestionRelation,
      planner: SparkConnectPlanner): LogicalPlan = {

    val spark = planner.sessionHolder.session
    val inputDf = deserializeInputRelation(req.getInputRelation, planner)

    // Build suggestion runner
    var suggestionRunner = ConstraintSuggestionRunner().onData(inputDf)

    // Add constraint rules
    val ruleNames = req.getConstraintRulesList.asScala.toSeq
    ruleNames.foreach { ruleName =>
      suggestionRunner = suggestionRunner.addConstraintRules(parseConstraintRules(ruleName))
    }

    // Restrict to columns if specified
    val restrictToColumns = req.getRestrictToColumnsList.asScala.toSeq
    if (restrictToColumns.nonEmpty) {
      suggestionRunner = suggestionRunner.restrictToColumns(restrictToColumns)
    }

    // Set histogram threshold
    if (req.getLowCardinalityHistogramThreshold > 0) {
      suggestionRunner = suggestionRunner
        .withLowCardinalityHistogramThreshold(req.getLowCardinalityHistogramThreshold)
    }

    // Set KLL parameters if KLL profiling is enabled
    if (req.getEnableKllProfiling && req.hasKllParameters) {
      val kllParams = req.getKllParameters
      suggestionRunner = suggestionRunner.setKLLParameters(KLLParameters(
        kllParams.getSketchSize,
        kllParams.getShrinkingFactor,
        kllParams.getNumberOfBuckets
      ))
    } else if (req.getEnableKllProfiling) {
      // Use default KLL parameters (matching Python defaults)
      suggestionRunner = suggestionRunner.setKLLParameters(KLLParameters(2048, 0.64, 64))
    }

    // Set predefined types if provided
    val predefinedTypes = req.getPredefinedTypesMap.asScala.toMap
    if (predefinedTypes.nonEmpty) {
      val typeMap = predefinedTypes.map { case (col, typeName) =>
        col -> parseDataType(typeName)
      }
      suggestionRunner = suggestionRunner.setPredefinedTypes(typeMap)
    }

    // Set train/test split if specified
    if (req.getTestsetRatio > 0) {
      val seed = if (req.getTestsetSplitRandomSeed != 0) {
        Some(req.getTestsetSplitRandomSeed)
      } else {
        None
      }
      suggestionRunner = suggestionRunner.useTrainTestSplitWithTestsetRatio(req.getTestsetRatio, seed)
    }

    // Run suggestion analysis
    val result = suggestionRunner.run()

    // Convert to DataFrame
    constraintSuggestionsToDataFrame(spark, result).queryExecution.logical
  }

  /**
   * Parse constraint rule name to Deequ Rules.
   */
  private def parseConstraintRules(
      ruleName: String): Seq[com.amazon.deequ.suggestions.rules.ConstraintRule[ColumnProfile]] = {
    ruleName.toUpperCase match {
      case "DEFAULT" => Rules.DEFAULT
      case "STRING" => Rules.STRING
      case "NUMERICAL" => Rules.NUMERICAL
      case "COMMON" => Rules.COMMON
      case "EXTENDED" => Rules.EXTENDED
      case _ => Rules.DEFAULT
    }
  }

  /**
   * Parse data type string to DataTypeInstances.
   */
  private def parseDataType(typeName: String): DataTypeInstances.Value = {
    typeName.toLowerCase match {
      case "string" => DataTypeInstances.String
      case "integer" | "int" => DataTypeInstances.Integral
      case "long" => DataTypeInstances.Integral
      case "double" | "float" => DataTypeInstances.Fractional
      case "boolean" | "bool" => DataTypeInstances.Boolean
      case _ => DataTypeInstances.Unknown
    }
  }

  /**
   * Convert ColumnProfiles to a DataFrame.
   */
  private def columnProfilesToDataFrame(
      spark: SparkSession,
      profiles: ColumnProfiles): DataFrame = {

    import spark.implicits._

    val gson = new Gson()

    val rows = profiles.profiles.values.map { profile =>
      val typeCounts = if (profile.typeCounts.nonEmpty) {
        Some(gson.toJson(profile.typeCounts.asJava))
      } else {
        None
      }

      val histogram = profile.histogram.map { hist =>
        gson.toJson(hist.values.map { case (k, v) =>
          Map("value" -> k, "count" -> v.absolute, "ratio" -> v.ratio).asJava
        }.toSeq.asJava)
      }

      // Extract numeric-specific fields
      val (mean, min, max, sum, stdDev, kllBuckets, approxPercentiles) = profile match {
        case np: NumericColumnProfile =>
          val kll = np.kll.map { kllDist =>
            gson.toJson(kllDist.buckets.map { bucket =>
              Map("low" -> bucket.lowValue, "high" -> bucket.highValue, "count" -> bucket.count).asJava
            }.asJava)
          }
          val percentiles = np.approxPercentiles.map { p =>
            gson.toJson(p.asJava)
          }
          (np.mean, np.minimum, np.maximum, np.sum, np.stdDev, kll, percentiles)
        case _ =>
          (None, None, None, None, None, None, None)
      }

      (
        profile.column,
        profile.completeness,
        profile.approximateNumDistinctValues,
        profile.dataType.toString,
        profile.isDataTypeInferred,
        typeCounts.orNull,
        histogram.orNull,
        mean.map(java.lang.Double.valueOf).orNull,
        min.map(java.lang.Double.valueOf).orNull,
        max.map(java.lang.Double.valueOf).orNull,
        sum.map(java.lang.Double.valueOf).orNull,
        stdDev.map(java.lang.Double.valueOf).orNull,
        approxPercentiles.orNull,
        kllBuckets.orNull
      )
    }.toSeq

    rows.toDF(
      "column",
      "completeness",
      "approx_distinct_values",
      "data_type",
      "is_data_type_inferred",
      "type_counts",
      "histogram",
      "mean",
      "minimum",
      "maximum",
      "sum",
      "std_dev",
      "approx_percentiles",
      "kll_buckets"
    )
  }

  /**
   * Convert ConstraintSuggestionResult to a DataFrame.
   */
  private def constraintSuggestionsToDataFrame(
      spark: SparkSession,
      result: com.amazon.deequ.suggestions.ConstraintSuggestionResult): DataFrame = {

    import spark.implicits._

    // Flatten all suggestions
    val allSuggestions = result.constraintSuggestions.values.flatten.toSeq

    // Get evaluation results if available
    val evaluationResults = result.verificationResult.map { verificationResult =>
      verificationResult.checkResults.values.headOption.map { checkResult =>
        checkResult.constraintResults.map { cr =>
          (cr.status.toString, cr.metric.flatMap(_.value.toOption))
        }
      }.getOrElse(Seq.empty)
    }.getOrElse(Seq.empty)

    val rows = allSuggestions.zipWithIndex.map { case (suggestion, idx) =>
      val evaluationStatus = evaluationResults.lift(idx).map(_._1).orNull
      val evaluationMetricValue = evaluationResults.lift(idx)
        .flatMap(_._2)
        .map(v => java.lang.Double.valueOf(v.asInstanceOf[Double]))
        .orNull

      (
        suggestion.columnName,
        suggestion.constraint.toString,
        suggestion.currentValue,
        suggestion.description,
        suggestion.suggestingRule.toString,
        convertScalaToPythonCode(suggestion.codeForConstraint),
        evaluationStatus,
        evaluationMetricValue
      )
    }

    rows.toDF(
      "column_name",
      "constraint_name",
      "current_value",
      "description",
      "suggesting_rule",
      "code_for_constraint",
      "evaluation_status",
      "evaluation_metric_value"
    )
  }

  /**
   * Convert Scala-style code to Python-like syntax.
   */
  private def convertScalaToPythonCode(code: String): String = {
    var result = code
    // Unwrap Some(...)
    result = result.replaceAll("""Some\(([^)]+)\)""", "$1")
    // Convert Array(...) to [...]
    result = result.replaceAll("""Array\(([^)]+)\)""", "[$1]")
    // Convert Seq(...) to [...]
    result = result.replaceAll("""Seq\(([^)]+)\)""", "[$1]")
    result
  }
}
