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

import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.linalg.{Vectors, Vector => SparkVector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

/**
  * Predictor that predicts the most frequent label and ignores the input features.
  * To be used as model for mode imputation in the Imputer spark.ml component
  *
  * @param uid
  */
class MostFrequentLabelClassifier(override val uid: String)
  extends Classifier[SparkVector, MostFrequentLabelClassifier, MostFrequentLabelClassifierModel]
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("most-frequent-label-classifier"))

  def train(dataset: Dataset[_]): MostFrequentLabelClassifierModel = {

    import dataset.sparkSession.implicits._

    val mostFrequentLabel: Double = dataset.groupBy($(labelCol))
      .count
      .orderBy(desc("count"))
      .select($(labelCol))
      .as[Double]
      .first

    copyValues(new MostFrequentLabelClassifierModel(uid, mostFrequentLabel).setParent(this))

  }

  /**
    * Validates and transforms the input schema with the provided param map.
    *
    * @param schema            input schema, e.g., [[org.apache.spark.mllib.linalg.VectorUDT]]
    *                          for vector features.
    * @return output schema
    */
  override def transformSchema(schema: StructType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    StructType(schema.fields :+ StructField($(predictionCol), DoubleType, nullable = false))
  }

  override def copy(extra: ParamMap): MostFrequentLabelClassifier = defaultCopy(extra)

}

object MostFrequentLabelClassifier extends DefaultParamsReadable[MostFrequentLabelClassifier] {
  override def load(path: String): MostFrequentLabelClassifier = super.load(path)
}

class MostFrequentLabelClassifierModel(override val uid: String, val mostFrequentLabel: Double)
  extends ClassificationModel[SparkVector, MostFrequentLabelClassifierModel] with MLWritable {

  def this(mostFrequentLabel: Double) = {
    this(Identifiable.randomUID("most-frequent-label-classifier"), mostFrequentLabel)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset.withColumn($(predictionCol), lit(mostFrequentLabel))
  }

  override def numClasses: Int = 1

  override def predictRaw(features: SparkVector): SparkVector = {
    Vectors.dense(mostFrequentLabel)
  }

  /**
    * Validates and transforms the input schema with the provided param map.
    *
    * @param schema            input schema, e.g., [[org.apache.spark.mllib.linalg.VectorUDT]]
    *                          for vector features.
    * @return output schema
    */
  override def transformSchema(schema: StructType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    StructType(schema.fields :+ StructField($(predictionCol), DoubleType, nullable = false))
  }

  override def copy(extra: ParamMap): MostFrequentLabelClassifierModel = {
    val that = this.getClass.getConstructor(classOf[String]).newInstance(uid)
    copyValues(that, extra).asInstanceOf[MostFrequentLabelClassifierModel]
  }

  override def write: MLWriter = MostFrequentLabelClassifierModel.write(this)
}

object MostFrequentLabelClassifierModel extends MLReadable[MostFrequentLabelClassifierModel] {
  override def read: MLReader[MostFrequentLabelClassifierModel] = new ModeImputationModelReader

  override def load(path: String): MostFrequentLabelClassifierModel = super.load(path)

  def write(model: MostFrequentLabelClassifierModel): MLWriter = {
    new MostFrequentLabelClassifierModelWriter(model)
  }

  // TODO why do we need those when they are not implemented?
  /** [[MLWriter]] instance for [[MostFrequentLabelClassifierModel]] */
  private[MostFrequentLabelClassifierModel] class MostFrequentLabelClassifierModelWriter(
      instance: MostFrequentLabelClassifierModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      throw new NotImplementedError()
    }
  }

  private[MostFrequentLabelClassifierModel] class ModeImputationModelReader
    extends MLReader[MostFrequentLabelClassifierModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MostFrequentLabelClassifierModel].getName

    override def load(path: String): MostFrequentLabelClassifierModel = {
      throw new NotImplementedError()
    }
  }

}

