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

package org.apache.spark.ml.param.shared.deequ

import org.apache.spark.ml.param.shared.{
  HasInputCol => SparkHasInputCol,
  HasInputCols => SparkHasInputCols,
  HasMaxIter => SparkHasMaxIter,
  HasOutputCol => SparkHasOutputCol,
  HasPredictionCol => SparkHasPredictionCol,
  HasLabelCol => SparkHasLabelCol
}

import org.apache.spark.ml.{PredictorParams => SparkPredictorParams}

trait HasInputCol extends SparkHasInputCol
trait HasInputCols extends SparkHasInputCols
trait HasOutputCol extends SparkHasOutputCol
trait HasPredictionCol extends SparkHasPredictionCol
trait HasLabelCol extends SparkHasLabelCol
trait HasMaxIter extends SparkHasMaxIter
trait PredictorParams extends SparkPredictorParams
