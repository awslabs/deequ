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
import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.checks.Check
import com.amazon.deequ.connect.proto._
import com.amazon.deequ.metrics.Metric
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
}
