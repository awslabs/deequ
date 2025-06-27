# Anomaly detection with extended results

Using the `addAnomalyCheckWithExtendedResults` method instead of the original `addAnomalyCheck`method, you can get more 
detailed results about the anomaly detection result from the newly created metric. You can get details such as:

- dataMetricValue: The metric value that is the data point.
- anomalyMetricValue: The metric value that is being checked for the anomaly detection strategy, which isn't always equal to the dataMetricValue. 
- anomalyCheckRange: The range of bounds used in the anomaly check, the anomalyMetricValue is compared to this range. 
- isAnomaly: If the anomalyMetricValue is outside the anomalyCheckRange, this is true.
- confidence: The confidence of the anomaly detection.
- detail: An optional detail message.

These are contained within the AnomalyDetectionDataPoint class.
```scala
class AnomalyDetectionDataPoint(
val dataMetricValue: Double,
val anomalyMetricValue: Double,
val anomalyCheckRange: BoundedRange,
val isAnomaly: Boolean,
val confidence: Double,
val detail: Option[String])

case class BoundedRange(lowerBound: Bound, upperBound: Bound)

case class Bound(value: Double, inclusive: Boolean)
```

In terms of accessing the result, the AnomalyDetectionDataPoint is wrapped in an AnomalyDetectionExtendedResult class 
that is an optional field in the ConstraintResult class. The ConstraintResult class is a class that contains the 
results of a constraint check. 

```scala
case class ConstraintResult(
    constraint: Constraint,
    status: ConstraintStatus.Value,
    message: Option[String] = None,
    metric: Option[Metric[_]] = None,
    anomalyDetectionExtendedResultOption: Option[AnomalyDetectionExtendedResult] = None)

case class  AnomalyDetectionExtendedResult(anomalyDetectionDataPoint: AnomalyDetectionDataPoint)
```


In order to get extended results you need to run your verification suite with 
the `addAnomalyCheckWithExtendedResults` method, which has the same method signature as the original `addAnomalyCheck` 
method.

```scala
val result = VerificationSuite()
  .onData(yesterdaysDataset)
  .useRepository(metricsRepository)
  .saveOrAppendResult(yesterdaysKey)
  .addAnomalyCheckWithExtendedResults(
    RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),
    Size())
  .run()

val anomalyDetectionExtendedResult: AnomalyDetectionExtendedResult = result.checkResults.head._2.constraintResults.head
  .anomalyDetectionExtendedResultOption.getOrElse("placeholder to do something else")

val anomalyDetectionDataPoint: AnomalyDetectionDataPoint = anomalyDetectionExtendedResult.anomalyDetectionDataPoint
```

You can access the values of the anomaly detection extended results like the anomalyMetricValue and anomalyCheckRange.
```scala
println(s"Anomaly check range: ${anomalyDetectionDataPoint.anomalyCheckRange}")
println(s"Anomaly metric value: ${anomalyDetectionDataPoint.anomalyMetricValue}")
```

```
Anomaly check range: BoundedRange(Bound(-2.0,true),Bound(2.0,true))
Anomaly metric value: 4.5
```

An [executable version of this example with extended results](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/AnomalyDetectionWithExtendedResultsExample.scala) is available as part of our code base.
