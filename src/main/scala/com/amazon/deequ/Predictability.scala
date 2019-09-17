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

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Predictability {

  private[this] val numHashBuckets = 10000
  private[this] val ngramSize = 3
  private[this] val maxNumLabels = 100

  def compute(dataset: DataFrame, featureCols: Array[String], targetAttribute: String): Double = {

    val featuresText = s"${targetAttribute}_features_text"
    val featuresTokens = s"${targetAttribute}_features_tokens"
    val featureNgrams = s"${targetAttribute}_ngrams"

    // get the labels (value tokens of attribute) and features (all values of other features concatenated) */
    val datasetWithConcatenatedFeatures = dataset
        .na.drop(Seq(targetAttribute))
        .select(
          col(targetAttribute),
          clean(concat_ws(" ", featureCols.map { c => concat_ws(c + "_", col(c)) }: _*)).alias(featuresText))
    //TODO maybe allow users to cache this?

    // Keep only the most frequent labels */
    val labels = datasetWithConcatenatedFeatures
      .groupBy(targetAttribute)
      .count
      .orderBy(desc("count"))
      .limit(maxNumLabels)
      .take(maxNumLabels)
      .map { row: Row => row.getString(0) }

    // transform labels to label idx and drop those that are not amongst top [[dataRangeSize]] most frequent labels
    val exampleData = datasetWithConcatenatedFeatures
      .select(labelIndex(labels)(col(targetAttribute)).alias("label"), col(featuresText))
      .where(col("label") > -1)
      .persist


    // label converter from predicted labels to label tokens
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labels)

    // tokenizer for string features
    val tokenizer = new RegexTokenizer()
      .setPattern("")
      .setInputCol(featuresText)
      .setOutputCol(featuresTokens)

    val ngram = new NGram()
      .setN(ngramSize)
      .setInputCol(featuresTokens)
      .setOutputCol(featureNgrams)

    // hashing vectorizer for features
    val hashingTransformer = new HashingTF()
      .setInputCol(featureNgrams)
      .setOutputCol("features")
      .setNumFeatures(numHashBuckets)

    // It's ok to do the train test split only here, as all our features are computed tuple-at-a-time
    val Array(training, test) = exampleData.randomSplit(Array(0.8, 0.2))

    val pipeline = new Pipeline()
      .setStages(Array(
        tokenizer,
        ngram,
        hashingTransformer,
        new NaiveBayes(),
        labelConverter))
      .fit(training)

    val testWithPredictions = pipeline.transform(test)

    val predictionStats = testWithPredictions
      .select(
        sum(expr("prediction == label").cast(IntegerType)).as("correct"),
        sum(expr("prediction != label").cast(IntegerType)).as("incorrect"))
      .collect()
      .head

    val correct = predictionStats.getLong(0)
    val incorrect = predictionStats.getLong(1)

    val accuracy = correct.toDouble / (correct + incorrect)

    accuracy
  }

  /** regular expression for filtering out html tags */
  private[deequ] val htmlTagPattern = "<[^<]+>".r

  /** helper udf to clean strings */
  private[deequ] val clean = udf(cleanString(_: String))


  /** helper to replace label token string by index of label */
  private[deequ] def labelIndex(labels: Array[String]) = {
    udf((input: String) => labels.indexOf(input).toDouble)
  }

  private[deequ] def cleanString(s: String): String = {
    htmlTagPattern.replaceAllIn(s, "")
      .toLowerCase
      .split("/").map(_.trim).mkString(" ")
      .split("-").map(_.trim).mkString(" ")
      .split(",").map(_.trim).mkString(" ")
  }

}





