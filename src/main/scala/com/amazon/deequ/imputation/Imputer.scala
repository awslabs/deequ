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

package com.amazon.deequ.imputation

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IndexToString, Tokenizer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.deequ.HasInputCols
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineModel}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.math.pow

case class ModelWithLabelHistogram(
    attribute: String,
    model: PipelineModel,
    labelHistogram: Array[(String, Long)])

trait ImputerHelperFunctions {

  /** regular expression for filtering out html tags */
  val htmlTagPattern = "<[^<]+>" r
  /** helper udf to clean strings */
  val cleanStringsUdf = udf(cleanString(_: String))
  /** helper to merge to-be-imputed column's existing values with imputed */
  val mergeImputedUdf = udf { (entry: String, imputed: String) =>
    if (entry == null) imputed else entry }

  /** Add suffix clean to string */
  def transformAttributeName(att: String): String = att + "_clean"

  /** helper to replace label token string by idx of label histogram */
  def label2LabelIdx(labelHistogram: Array[String]): UserDefinedFunction = {
    udf { (input: String) => labelHistogram.indexOf(input).toDouble }
  }
  /**
    * Some minimal cleaning on a string
    *
    * @param s
    * @return lowercased string with html tags and the chars /-, removed
    */
  def cleanString(s: String): String = {
    val sWithoutHtml = htmlTagPattern.replaceAllIn(s, "")
    sWithoutHtml.toLowerCase
      .split("/").map(_.trim).mkString(" ")
      .split("-").map(_.trim).mkString(" ")
      .split(",").map(_.trim).mkString(" ")
  }

}

trait ImputerModelParams extends Params with HasInputCols {

  val imputedSuffix: Param[String] = new Param[String](this, "imputedSuffix",
    "Suffix for for to-be-imputed columns")
  val featuresCols: Param[Array[Array[String]]] = new Param[Array[Array[String]]](this,
    "featuresCols", "columns to be used for imputation of input columns; needs to be of same " +
      "length as inputCols/outputCols; default: all columns but the to-be-imputed column and the " +
      "key/index column")

  /** the supervised machine learning model to impute missing values */
  val model: Param[Estimator[_]] = new Param(this, "model",
    "model to use for imputation on input column(s)")
  val outputCols: Param[Array[String]] = new Param[Array[String]](this, "outputCols",
    "output column names; needs to be of same length as inputCols")
  val numHashBuckets: IntParam = new IntParam(this, "numHashBuckets",
      "max number of bins for the feature hashing vectorizer", ParamValidators.gt(0))
  val dataRangeSize: IntParam = new IntParam(this, "dataRangeSize",
      "max number of labels (attribute values to impute) after ranking labels by frequency",
      ParamValidators.gt(0))

  def setImputedSuffix(value: String): this.type = set(imputedSuffix, value)

  def getImputedSuffix: String = $(imputedSuffix)

  def getFeaturesCols: Array[Array[String]] = $(featuresCols)

  def setModel(value: Estimator[_]): this.type = set(model, value)

  def getModel: Estimator[_] = $(model)

  def getOutputCols: Array[String] = $(outputCols)

  def setNumHashBuckets(value: Int): this.type = set(numHashBuckets, value)

  def setDataRangeSize(value: Int): this.type = set(dataRangeSize, value)

  /**
    * Checks schema of input data set and sets default values
    *
    * @param schema Schema of input dataset / dataframe
    * @return new schema with input / output columns set
    */
  protected def validateAndTransformSchema(schema: StructType): StructType = {

    val log: Logger = LoggerFactory.getLogger(this.getClass)

    // if there are no input columns provided, take all nullable cols as default input columns
    // (will be imputed)
    if ($(inputCols).length == 0) {
      log.info("Setting input cols to all nullable columns")
      setInputCols(schema.fields.filter(_.nullable).map(_.name): _*)
    }

    if ($(outputCols).length == 0) {
      log.info(s"Setting output col names to input column names + ${$(imputedSuffix)}")
      setOutputCols(getInputCols.map(_ + $(imputedSuffix)): _*)
    }

    require($(inputCols).length == $(outputCols).length, s"Number of input columns " +
      s"(${$(inputCols).length}) must match number of output columns (${$(outputCols).length})")

    val fieldNames = schema.fieldNames

    if ($(featuresCols).length > 0) {
      require($(featuresCols).length == $(inputCols).length, s"When setting featuresCols, the " +
        s"number of Array[String] elements in it must match the number of inputCols " +
        s"(${$(inputCols).length}}), but was: ${$(featuresCols).length}}")

      log.info(s"Taking featuresCols (${$(featuresCols)} column sets) as inputs to imputation")

      $(inputCols).zip($(featuresCols)).foreach { case (origCol, featureCols) =>
        // verify that each of the feature columns for imputing origCol is amongst the columns
        // of the input dataset.
        featureCols.foreach { featureCol =>
          require(fieldNames.contains(featureCol),
            s"Feature column ${featureCol} for imputation of ${origCol} is missing")
          require(!featureCols.contains(origCol),
            s"To-be-imputed column ${origCol} is amongst feature columns")
        }
      }
    } else {
      val featureCols = $(inputCols).map { origCol =>
        log.info(s"Taking all columns but to-be-imputed column (${origCol}) " +
          s"as features to imputation of ${origCol}")
        fieldNames.toSet.diff(Set(origCol)).toArray
      }
      setFeaturesCols(featureCols: _*)
    }

    val newFields = $(inputCols).zip($(outputCols)).map { case (origCol, imputedCol) =>
      // verify that the column to impute is amongst the input columns.
      require(fieldNames.contains(origCol),
        s"Column to impute ($origCol) is not amongst input columns.")
      // verify that the name of the imputed column does not exist yet.
      require(!fieldNames.contains(imputedCol),
        s"Output column $imputedCol already exists.")

      StructField(imputedCol, schema(origCol).dataType, nullable = false)
    }

    StructType(schema.fields ++ newFields)
  }

  def setOutputCols(value: String*): this.type = set(outputCols, value.toArray)

  def setInputCols(value: String*): this.type = set(inputCols, value.toArray)

  setDefault(
    dataRangeSize -> 100,
    numHashBuckets -> pow(2, 20).toInt,
    featuresCols -> Array[Array[String]](),
    imputedSuffix -> "_imputed",
    inputCols -> Array[String](),
    outputCols -> Array[String](),
    model -> new NaiveBayes()
  )

  def setFeaturesCols(value: Array[String]*): this.type = set(featuresCols, value.toArray)
}

/**
  * Imputer imputes the missing string values of a column of strings
  *
  */
class Imputer(override val uid: String) extends Estimator[ImputerModel]
  with ImputerModelParams with ImputerHelperFunctions with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("imputation"))

  override def fit(dataset: Dataset[_]): ImputerModel = {

    transformSchema(dataset.schema, logging = true)

    val trainedModels = $(inputCols).zip($(featuresCols))
      .map { case (att: String, featureCols: Array[String]) =>

        val attHashed = att + "_hashed"

        val featuresText = att + "_features_text"
        val featuresTokens = att + "_features_tokens"
        val featuresHashed = att + "_features_hashed"

        // get the labels (value tokens of attribute) and features (all values of other
        // features concatenated) */
        // TODO: when concatenating strings that should/will be tokenized downstream,
        // TODO each token should be prefixed
        val dsCleanLabelsConcatenatedFeatures = {
          dataset.na.drop(Seq(att))
            .select(
              cleanStringsUdf(col(att)).alias(transformAttributeName(att)),
              cleanStringsUdf(concat_ws(" ",
                featureCols.map { c => concat_ws(c + "_", col(c)) }: _*)).alias(featuresText))
        }.persist

        // Compute label histogram and keep only the top [[dataRangeSize]] most frequent labels */
        val labelHistogram = dsCleanLabelsConcatenatedFeatures
          .groupBy(transformAttributeName(att))
          .count
          .orderBy(desc("count"))
          .take($(dataRangeSize))
          .map { row: Row => (row.getString(0), row.getLong(1)) }

        val labels = labelHistogram.map(_._1)

        // transform labels to label idx and drop those that are not amongst top [[dataRangeSize]]
        // most frequent labels
        val ds = dsCleanLabelsConcatenatedFeatures.select(
          label2LabelIdx(labels)(col(transformAttributeName(att))).alias("label"),
          col(featuresText))
          .where(col("label") > -1)
          .persist

        dsCleanLabelsConcatenatedFeatures.unpersist

        log.info(s"Found ${ds.count} rows for attribute ${att} that contain the " +
          s"top ${$(dataRangeSize)} frequent labels " +
          s"(max count: ${labelHistogram.head} / min count: ${labelHistogram.last})")

        // label converter from predicted labels to label tokens
        val labelConverter = {
          new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(labels)
        }

        // tokenizer for string features
        val tokenizer = {
          new Tokenizer()
            .setInputCol(featuresText)
            .setOutputCol(featuresTokens)
        }

        // hashing vectorizer for features
        val hashingTransformer = {
          new HashingTF()
            .setInputCol(featuresTokens)
            .setOutputCol("features")
            .setNumFeatures($(numHashBuckets))
        }

        val pipeline = {
          new Pipeline()
            .setStages(Array(
              tokenizer,
              hashingTransformer,
              $(model),
              labelConverter))
        }.fit(ds)

        ds.unpersist

        ModelWithLabelHistogram(att, pipeline, labelHistogram)
    }

    copyValues(new ImputerModel(uid, trainedModels).setParent(this))

  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def copy(extra: ParamMap): Imputer = defaultCopy(extra)

}

object Imputer extends DefaultParamsReadable[Imputer] {
  override def load(path: String): Imputer = super.load(path)
}

class ImputerModel(
  override val uid: String,
  imputationModels: Seq[ModelWithLabelHistogram])
  extends Model[ImputerModel] with ImputerModelParams with ImputerHelperFunctions with MLWritable {

  def this(
    models: Seq[ModelWithLabelHistogram]) =
    this(Identifiable.randomUID("imputer"), models)

  override def transform(dataset: Dataset[_]): DataFrame = {

    transformSchema(dataset.schema, logging = true)

    var outDf = dataset

    imputationModels.zip($(outputCols)).zip($(featuresCols)).foreach {

      case ((model: ModelWithLabelHistogram, outputCol: String), featureCols: Array[String]) =>


        val featuresText = model.attribute + "_features_text"
        val featuresTokens = model.attribute + "_features_tokens"
        val featuresHashed = model.attribute + "_features_hashed"

        val inputCol = col(model.attribute)

        // impute missing values
        outDf = model.model
          .transform(
            outDf
              .withColumn(featuresText,
                // TODO: use same udf for fit and transform
                cleanStringsUdf(concat_ws(" ",
                  featureCols.map { c => concat_ws(c + "_", col(c)) }: _*)))
          )
          .withColumn(outputCol, mergeImputedUdf(inputCol, col("predictedLabel")))
          .drop("predictedLabel", "features", "prediction", "rawPrediction", "probability",
            featuresText, featuresTokens, featuresHashed)
      // TODO: include probability column for models with probabilistic outputs
    }

    outDf.toDF
  }

  /**
    * Returns the histogram of labels / attribute-values, also referred to as DataRange of the
    * imputation of a column
    *
    * @param att column of which to return the data range (= label histogram)
    * @return data range
    */
  def dataRange(att: String): Array[(String, Long)] = {
    val dataRange = imputationModels.filter(_.attribute == att)
    require(dataRange.nonEmpty, s"Could not find data range for column $att")
    dataRange.head.labelHistogram
  }

  /**
    * Returns the imputation model of a column
    *
    * @param att column of which to return the model
    * @return spark.ml Model
    */
  def imputationModel(att: String): Model[_] = {
    val attModel = imputationModels.filter(_.attribute == att)
    require(attModel.nonEmpty, s"Could not find model for column $att")
    attModel.head.model
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def copy(extra: ParamMap): ImputerModel = {
    val that = this.getClass.getConstructor(classOf[String]).newInstance(uid)
    copyValues(that, extra).asInstanceOf[ImputerModel]
  }

  override def write: MLWriter = ImputerModel.write(this)
}

object ImputerModel extends MLReadable[ImputerModel] {
  override def read: MLReader[ImputerModel] = new ImputerModelReader

  override def load(path: String): ImputerModel = super.load(path)

   def write(model: ImputerModel): MLWriter = new ImputerModelWriter(model)

  /** [[MLWriter]] instance for [[ImputerModel]] */
  private[ImputerModel] class ImputerModelWriter(instance: ImputerModel) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      this.sc.parallelize(Seq(instance), 1).saveAsObjectFile(path)
    }
  }

  private[ImputerModel] class ImputerModelReader extends MLReader[ImputerModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[ImputerModel].getName

    override def load(path: String): ImputerModel = this.sc.objectFile[ImputerModel](path).first()
  }
}
