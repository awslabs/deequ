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


/**
 * Anomaly Detection Data Point class (previously Anomaly)
 *
 * @param dataMetricValue The metric value that is the data point
 * @param anomalyMetricValue The metric value that is being used in the anomaly calculation.
 *                           This usually aligns with dataMetricValue but not always,
 *                           like in a rate of change strategy where the rate of change is the anomaly metric
 *                           which may not equal the actual data point value
 * @param anomalyThreshold The thresholds used in the anomaly check, the anomalyMetricValue is
 *                         compared to this threshold
 * @param isAnomaly If the data point is an anomaly
 * @param confidence TODO fill in more info about this
 * @param detail Detailed error message
 */
class AnomalyDetectionDataPoint(
                                 val dataMetricValue: Double,
                                 val anomalyMetricValue: Double,
                                 val anomalyThreshold: AnomalyThreshold,
                                 val isAnomaly: Boolean,
                                 val confidence: Double,
                                 val detail: Option[String]) {

  def canEqual(that: Any): Boolean = {
    that.isInstanceOf[AnomalyDetectionDataPoint]
  }

  /**
    * Tests anomalyDetectionDataPoints for equality. Ignores detailed explanation.
    *
    * @param obj The object/ anomaly to compare against
    * @return true, if and only if the dataMetricValue, anomalyMetricValue, anomalyThreshold, isAnomaly
   *         and confidence are the same
    */
  override def equals(obj: Any): Boolean = {
    obj match {
      case anomaly: AnomalyDetectionDataPoint => anomaly.dataMetricValue == dataMetricValue &&
        anomaly.anomalyMetricValue == anomalyMetricValue &&
        anomaly.anomalyThreshold == anomalyThreshold &&
        anomaly.isAnomaly == isAnomaly &&
        anomaly.confidence == confidence
      case _ => false
    }
  }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + dataMetricValue.hashCode()
    result = prime * result + anomalyMetricValue.hashCode()
    result = prime * result + anomalyThreshold.hashCode()
    result = prime * result + isAnomaly.hashCode()
    result = prime * result + confidence.hashCode()
    result
  }

}

object AnomalyDetectionDataPoint {
  def apply(dataMetricValue: Double, anomalyMetricValue: Double,
            anomalyThreshold: AnomalyThreshold = AnomalyThreshold(), isAnomaly: Boolean = false,
            confidence: Double, detail: Option[String] = None
           ): AnomalyDetectionDataPoint = {
    new AnomalyDetectionDataPoint(dataMetricValue, anomalyMetricValue, anomalyThreshold, isAnomaly, confidence, detail)
  }
}


/**
 * AnomalyThreshold class
 * Defines threshold for the anomaly detection, defaults to inclusive bounds of Double.Min and Double.Max
 * @param upperBound The upper bound or threshold
 * @param lowerBound The lower bound or threshold
 */
case class AnomalyThreshold(lowerBound: Bound = Bound(Double.MinValue), upperBound: Bound = Bound(Double.MaxValue))

/**
 * Bound Class
 * Class representing a threshold/bound, with value and inclusive/exclusive boolean
 * @param value The value of the bound as a Double
 * @param inclusive Boolean indicating if the Bound is inclusive or not
 */
case class Bound(value: Double, inclusive: Boolean = true)



/**
 * AnomalyDetectionResult Class
 * This class is returned from the detectAnomaliesInHistory function
 * @param anomalyDetectionDataPointSequence The sequence of (timestamp, anomaly) pairs
 */
case class AnomalyDetectionResult(anomalyDetectionDataPointSequence: Seq[(Long, AnomalyDetectionDataPoint)] = Seq.empty)

/**
 * AnomalyDetectionAssertionResult Class
 * This class is returned by the anomaly detection assertion function
 * @param hasNoAnomaly Boolean indicating if anomaly was detected
 * @param anomalyDetectionMetadata Anomaly Detection metadata class containing anomaly details
 *                                 about the data point being checked
 */
case class AnomalyDetectionAssertionResult(hasNoAnomaly: Boolean, anomalyDetectionMetadata: AnomalyDetectionMetadata)


/**
 * AnomalyDetectionMetadata Class
 * This class containst anomaly detection metadata and is currently an optional field
 * in the ConstraintResult class that is exposed to users
 *
 * Currently, anomaly detection only runs on "newest" data point (referring to the dataframe being
 * run on by the verification suite) and not multiple data points, so this metadata class only contains
 * one anomalyDetectionDataPoint for now
 * In the future, if we allow the anomaly check to detect multiple points, we can return the anomalyDetectionResult
 * instead, which contains a sequence of (Long, AnomalyDetectionDataPoints)
 * @param anomalyDetectionDataPoint Anomaly detection data point
 */
case class AnomalyDetectionMetadata(anomalyDetectionDataPoint: AnomalyDetectionDataPoint)

