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

import scala.collection.JavaConverters._
import com.amazon.deequ.repository.JsonSerializationConstants.STRING_MAP_TYPE
import com.google.gson.GsonBuilder

import scala.collection.{Map, Seq, immutable}
import java.util.{ArrayList => JArrayList, Map => JMap}

object SimpleResultSerde {

  def serialize(successData: Seq[Map[String, Any]]): String = {

    val javaSuccessData = successData.map(_.asJava).asJava

    new GsonBuilder().serializeNulls.create
      .toJson(javaSuccessData, STRING_MAP_TYPE)
  }

  def deserialize(json: String): Seq[immutable.Map[String, Any]] = {
    new GsonBuilder().create
      .fromJson(json, STRING_MAP_TYPE)
      .asInstanceOf[JArrayList[JMap[String, String]]]
      .asScala
      .map(map => immutable.Map(map.asScala.toList: _*))
  }
}
