/** Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.repository

import com.google.gson.reflect.TypeToken

import java.lang.reflect.Type
import java.util.{List => JList, Map => JMap}

object JsonSerializationConstants {

  val STRING_MAP_TYPE: Type =
    new TypeToken[JList[JMap[String, Any]]]() {}.getType
  val STRING_LIST_TYPE: Type = new TypeToken[JList[String]]() {}.getType

  val ANALYZER_FIELD = "analyzer"
  val ANALYZER_NAME_FIELD = "analyzerName"
  val WHERE_FIELD = "where"
  val COLUMN_FIELD = "column"
  val COLUMNS_FIELD = "columns"
  val METRIC_MAP_FIELD = "metricMap"
  val METRIC_FIELD = "metric"
  val DATASET_DATE_FIELD = "dataSetDate"
  val TAGS_FIELD = "tags"
  val RESULT_KEY_FIELD = "resultKey"
  val ANALYZER_CONTEXT_FIELD = "analyzerContext"
}
