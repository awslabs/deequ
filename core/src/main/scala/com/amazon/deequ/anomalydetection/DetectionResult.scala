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

package com.amazon.deequ.anomalydetection

class Anomaly(
    val value: Option[Double],
    val confidence: Double,
    val detail: Option[String] = None) {

  def canEqual(that: Any): Boolean = {
    that.isInstanceOf[Anomaly]
  }

  /**
    * Tests anomalies for equality. Ignores detailed explanation.
    *
    * @param obj The object/ anomaly to compare against
    * @return true, if and only if the value and confidence are the same
    */
  override def equals(obj: Any): Boolean = {
    obj match {
      case anomaly: Anomaly => anomaly.value == value && anomaly.confidence == confidence
      case _ => false
    }
  }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (value == null) 0 else value.hashCode)
    prime * result + confidence.hashCode
  }

}

object Anomaly {
  def apply(value: Option[Double], confidence: Double, detail: Option[String] = None): Anomaly = {
    new Anomaly(value, confidence, detail)
  }
}

case class DetectionResult(anomalies: Seq[(Long, Anomaly)] = Seq.empty)
