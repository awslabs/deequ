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
import com.amazon.deequ.analyzers.{Analyzer, DataTypeInstances}
import com.amazon.deequ.connect.proto.{KLLParameters => ProtoKLLParameters}
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
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Spark Connect RelationPlugin for Deequ.
 *
 * Versioning follows Spark's RelationPlugin convention: the
 * protobuf type URL of the unpacked message is the wire-version discriminator.
 * Mismatched JAR/wheel pairs naturally fall through `relation.is(classOf[X])`
 * to `Optional.empty()`, letting Spark surface a clear "no handler found"
 * error.
 *
 * Register this plugin with:
 * --conf spark.connect.extensions.relation.classes=com.amazon.deequ.connect.DeequRelationPlugin
 */
class DeequRelationPlugin extends RelationPlugin {

  private val logger = LoggerFactory.getLogger(classOf[DeequRelationPlugin])

  override def transform(
      relation: ProtobufAny,
      planner: SparkConnectPlanner): Option[LogicalPlan] = {

    logger.debug("Received relation with type_url={}", relation.getTypeUrl)

    if (relation.is(classOf[DeequVerificationRelation])) {
      logger.debug("Handling verification request")
      return Some(handleVerification(relation.unpack(classOf[DeequVerificationRelation]), planner))
    }

    if (relation.is(classOf[DeequAnalysisRelation])) {
      logger.debug("Handling analysis request")
      return Some(handleAnalysis(relation.unpack(classOf[DeequAnalysisRelation]), planner))
    }

    if (relation.is(classOf[DeequColumnProfilerRelation])) {
      logger.debug("Handling column profiler request")
      return Some(handleColumnProfiler(relation.unpack(classOf[DeequColumnProfilerRelation]), planner))
    }

    if (relation.is(classOf[DeequConstraintSuggestionRelation])) {
      logger.debug("Handling constraint suggestion request")
      return Some(handleConstraintSuggestion(relation.unpack(classOf[DeequConstraintSuggestionRelation]), planner))
    }

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

    // Flatten all metrics (converts HistogramMetric, KLLMetric, etc. to
    // DoubleMetrics) then extract the Double values. Failed metrics
    // (DoubleMetric.value: Try[Double]) carry their exception via the
    // `error_message` column - symmetric with the verification path's
    // `constraint_message`. value is NaN whenever an error is set.
    val metrics = context.metricMap.toSeq.flatMap { case (analyzer, metric) =>
      metric.flatten().map { doubleMetric =>
        val (value, errorMessage) = doubleMetric.value match {
          case scala.util.Success(v) => (v, "")
          case scala.util.Failure(t) => (Double.NaN, t.getMessage)
        }
        (
          analyzer.toString,
          doubleMetric.entity.toString,
          doubleMetric.instance,
          doubleMetric.name,
          value,
          errorMessage
        )
      }
    }

    metrics.toDF(
      "analyzer",
      "entity",
      "instance",
      "name",
      "value",
      "error_message"
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

    // Set histogram threshold (presence-checked - see ADR-0001 F1)
    if (req.hasLowCardinalityHistogramThreshold) {
      profilerRunner = profilerRunner
        .withLowCardinalityHistogramThreshold(req.getLowCardinalityHistogramThreshold)
    }

    // Enable KLL profiling if requested.
    // Stage 2: clients send concrete parameters when KLL is enabled.
    if (req.getEnableKllProfiling) {
      profilerRunner = profilerRunner.withKLLProfiling()
      if (!req.hasKllParameters) {
        throw new IllegalArgumentException(
          "enable_kll_profiling=true requires kll_parameters to be set " +
            "(Stage 2: client must populate KLL defaults)")
      }
      profilerRunner = profilerRunner.setKLLParameters(Some(toKLLParameters(req.getKllParameters)))
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

    // Add constraint rules (typed enum - see CONTEXT.md)
    req.getConstraintRulesList.asScala.foreach { rule =>
      suggestionRunner = suggestionRunner.addConstraintRules(rulesFromProto(rule))
    }

    // Restrict to columns if specified
    val restrictToColumns = req.getRestrictToColumnsList.asScala.toSeq
    if (restrictToColumns.nonEmpty) {
      suggestionRunner = suggestionRunner.restrictToColumns(restrictToColumns)
    }

    // Set histogram threshold (presence-checked - see ADR-0001 F1)
    if (req.hasLowCardinalityHistogramThreshold) {
      suggestionRunner = suggestionRunner
        .withLowCardinalityHistogramThreshold(req.getLowCardinalityHistogramThreshold)
    }

    // Set KLL parameters if KLL profiling is enabled.
    // Stage 2: clients are required to send concrete parameters when KLL is
    // enabled (Python defaults via KLLParameters() dataclass). The previous
    // server-side library-default fallback is removed.
    if (req.getEnableKllProfiling) {
      if (!req.hasKllParameters) {
        throw new IllegalArgumentException(
          "enable_kll_profiling=true requires kll_parameters to be set " +
            "(Stage 2: client must populate KLL defaults)")
      }
      suggestionRunner = suggestionRunner.setKLLParameters(toKLLParameters(req.getKllParameters))
    }

    // Set predefined types if provided
    val predefinedTypes = req.getPredefinedTypesMap.asScala.toMap
    if (predefinedTypes.nonEmpty) {
      val typeMap = predefinedTypes.map { case (col, typeName) =>
        col -> parseDataType(typeName)
      }
      suggestionRunner = suggestionRunner.setPredefinedTypes(typeMap)
    }

    // Set train/test split - presence-checked so seed=0 is a legal user choice.
    if (req.hasTestsetRatio) {
      val seed = if (req.hasTestsetSplitRandomSeed) Some(req.getTestsetSplitRandomSeed) else None
      suggestionRunner = suggestionRunner.useTrainTestSplitWithTestsetRatio(req.getTestsetRatio, seed)
    }

    // Run suggestion analysis
    val result = suggestionRunner.run()

    // Convert to DataFrame
    constraintSuggestionsToDataFrame(spark, result).queryExecution.logical
  }

  /**
   * Map a `ConstraintRuleSet` enum value to the corresponding Deequ rule bundle.
   * Closed-set match - UNSPECIFIED is rejected because the schema's enum
   * always carries an explicit non-default variant under correct client use.
   */
  private def rulesFromProto(
      rule: ConstraintRuleSet)
    : Seq[com.amazon.deequ.suggestions.rules.ConstraintRule[ColumnProfile]] = rule match {
    case ConstraintRuleSet.CONSTRAINT_RULE_SET_DEFAULT => Rules.DEFAULT
    case ConstraintRuleSet.CONSTRAINT_RULE_SET_STRING => Rules.STRING
    case ConstraintRuleSet.CONSTRAINT_RULE_SET_NUMERICAL => Rules.NUMERICAL
    case ConstraintRuleSet.CONSTRAINT_RULE_SET_COMMON => Rules.COMMON
    case ConstraintRuleSet.CONSTRAINT_RULE_SET_EXTENDED => Rules.EXTENDED
    case ConstraintRuleSet.CONSTRAINT_RULE_SET_UNSPECIFIED | _ =>
      throw new IllegalArgumentException(
        s"Unspecified or unknown ConstraintRuleSet: $rule")
  }

  /**
   * Map a wire `ProtoKLLParameters` to the Deequ `KLLParameters` case class.
   * All three fields are required by the schema (proto3 scalars, no
   * `optional`); the server validates that they are positive - proto3's
   * implicit default of 0 is not a meaningful KLL parameter, so a zero
   * here means the client failed to populate concrete values.
   */
  private def toKLLParameters(p: ProtoKLLParameters): com.amazon.deequ.analyzers.KLLParameters = {
    if (p.getSketchSize <= 0 || p.getShrinkingFactor <= 0.0 || p.getNumberOfBuckets <= 0) {
      throw new IllegalArgumentException(
        s"KLLParameters must have positive sketch_size, shrinking_factor, and " +
          s"number_of_buckets (got ${p.getSketchSize}, ${p.getShrinkingFactor}, " +
          s"${p.getNumberOfBuckets}). Client must populate concrete values when " +
          "enable_kll_profiling is true.")
    }
    com.amazon.deequ.analyzers.KLLParameters(
      p.getSketchSize, p.getShrinkingFactor, p.getNumberOfBuckets)
  }

  /**
   * Parse data type string to DataTypeInstances.
   */
  /**
   * Parse a `predefined_types` value to a Deequ `DataTypeInstances`. Unknown
   * values are rejected with an actionable error rather than coerced to
   * `Unknown` - a typo like "strring" should surface as a client-side bug,
   * not a silent profile-with-Unknown-type.
   */
  private def parseDataType(typeName: String): DataTypeInstances.Value = {
    typeName.toLowerCase match {
      case "string" => DataTypeInstances.String
      case "integer" | "int" => DataTypeInstances.Integral
      case "long" => DataTypeInstances.Integral
      case "double" | "float" => DataTypeInstances.Fractional
      case "boolean" | "bool" => DataTypeInstances.Boolean
      case other =>
        throw new IllegalArgumentException(
          s"Unknown predefined_types value: '$typeName'. Supported values: " +
            "String, Integer, Int, Long, Double, Float, Boolean, Bool")
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

    // Flatten all suggestions. NOTE: `constraintSuggestions` is a Map keyed by
    // column name (built via groupBy in ConstraintSuggestionRunner), so its
    // iteration order is hash-based and DOES NOT match the original
    // construction order that built the verification check. Pairing by index
    // (a previous bug) silently associated wrong evaluation results.
    val allSuggestions = result.constraintSuggestions.values.flatten.toSeq

    // Build a Constraint-keyed lookup of evaluation results so each suggestion
    // joins to the result for its own constraint, regardless of iteration
    // order. ConstraintSuggestionRunner adds suggestions' Constraint instances
    // by reference into the verification Check, so object identity is stable.
    val evaluationByConstraint: Map[com.amazon.deequ.constraints.Constraint,
        (String, Option[Double])] =
      result.verificationResult.flatMap { verificationResult =>
        verificationResult.checkResults.values.headOption.map { checkResult =>
          checkResult.constraintResults.map { cr =>
            val maybeDouble: Option[Double] = cr.metric
              .flatMap(_.value.toOption)
              .collect { case d: Double => d }
            cr.constraint -> (cr.status.toString, maybeDouble)
          }.toMap
        }
      }.getOrElse(Map.empty)

    val rows = allSuggestions.map { suggestion =>
      val (evaluationStatus, evaluationMetricValue): (String, java.lang.Double) =
        evaluationByConstraint.get(suggestion.constraint) match {
          case Some((status, valueOpt)) =>
            (status, valueOpt.map(java.lang.Double.valueOf).orNull)
          case None =>
            (null, null)
        }

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
