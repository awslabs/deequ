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
 * The classes here provide the same anomaly detection functionality as in DetectionResult
 * but also provide extended results through details contained in the AnomalyDetectionDataPoint class.
 * See below.
 */

/**
 * Anomaly Detection Data Point class
 * This class is different from the Anomaly Class in that this class
 * wraps around all data points, not just anomalies, and provides extended results including
 * if the data point is an anomaly, and the thresholds used in the anomaly calculation.
 *
 * @param dataMetricValue The metric value that is the data point.
 * @param anomalyMetricValue The metric value that is being used in the anomaly calculation.
 *                           This usually aligns with dataMetricValue but not always,
 *                           like in a rate of change strategy where the rate of change is the anomaly metric
 *                           which may not equal the actual data point value.
 * @param anomalyThreshold The thresholds used in the anomaly check, the anomalyMetricValue is
 *                         compared to this threshold.
 * @param isAnomaly If the data point is an anomaly.
 * @param confidence Confidence of anomaly detection.
 * @param detail Detailed error message.
 */
class AnomalyDetectionDataPoint(
      val dataMetricValue: Double,
      val anomalyMetricValue: Double,
      val anomalyThreshold: Threshold,
      val isAnomaly: Boolean,
      val confidence: Double,
      val detail: Option[String])
  {

  def canEqual(that: Any): Boolean = {
    that.isInstanceOf[AnomalyDetectionDataPoint]
  }

  /**
   * Tests anomalyDetectionDataPoints for equality. Ignores detailed error message.
   *
   * @param obj The object/ anomaly to compare against.
   * @return true, if and only if the dataMetricValue, anomalyMetricValue, anomalyThreshold, isAnomaly
   *         and confidence are the same.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case anomaly: AnomalyDetectionDataPoint =>
        anomaly.dataMetricValue == dataMetricValue &&
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
            anomalyThreshold: Threshold = Threshold(), isAnomaly: Boolean = false,
            confidence: Double, detail: Option[String] = None
           ): AnomalyDetectionDataPoint = {
    new AnomalyDetectionDataPoint(dataMetricValue, anomalyMetricValue, anomalyThreshold, isAnomaly, confidence, detail)
  }
}


/**
 * Threshold class
 * Defines threshold for the anomaly detection, defaults to inclusive bounds of Double.Min and Double.Max.
 * @param upperBound The upper bound or threshold.
 * @param lowerBound The lower bound or threshold.
 */
case class Threshold(lowerBound: Bound = Bound(Double.MinValue), upperBound: Bound = Bound(Double.MaxValue))

/**
 * Bound Class
 * Class representing a threshold/bound, with value and inclusive/exclusive boolean/
 * @param value The value of the bound as a Double.
 * @param inclusive Boolean indicating if the Bound is inclusive or not.
 */
case class Bound(value: Double, inclusive: Boolean = true)



/**
 * ExtendedDetectionResult Class
 * This class is returned from the detectAnomaliesInHistoryWithExtendedResults function.
 * @param anomalyDetectionDataPointSequence The sequence of (timestamp, AnomalyDetectionDataPoint) pairs.
 */
case class ExtendedDetectionResult(anomalyDetectionDataPointSequence:
                                   Seq[(Long, AnomalyDetectionDataPoint)] = Seq.empty)


/**
 * AnomalyDetectionExtendedResult Class
 * This class contains anomaly detection extended results through a Sequence of AnomalyDetectionDataPoints.
 * This is currently an optional field in the ConstraintResult class that is exposed to users.
 *
 * Currently, anomaly detection only runs on "newest" data point (referring to the dataframe being
 * run on by the verification suite) and not multiple data points, so the returned sequence will contain
 * one AnomalyDetectionDataPoint.
 * In the future, if we allow the anomaly check to detect multiple points, the returned sequence
 * may be more than one AnomalyDetectionDataPoints.
 * @param anomalyDetectionDataPoints Sequence of AnomalyDetectionDataPoints.
 */
case class  AnomalyDetectionExtendedResult(anomalyDetectionDataPoints: Seq[AnomalyDetectionDataPoint])

/**
 * AnomalyDetectionAssertionResult Class
 * This class is returned by the assertion function Check.isNewestPointNonAnomalousWithExtendedResults.
 * @param hasNoAnomaly Boolean indicating if there was no anomaly detected.
 * @param anomalyDetectionExtendedResult AnomalyDetectionExtendedResults class.
 */
case class AnomalyDetectionAssertionResult(hasNoAnomaly: Boolean,
                                           anomalyDetectionExtendedResult: AnomalyDetectionExtendedResult)
