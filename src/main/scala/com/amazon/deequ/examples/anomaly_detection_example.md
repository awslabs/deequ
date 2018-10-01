# Anomaly detection

Very often, it is hard to exactly define what exact constraints we want to evaluate on our data. However, we often have a better understanding of how much change we expect in certain metrics of our data. Therefore, **deequ** supports anomaly detection for data quality metrics. The idea is that we regularly store the metrics of our data in a [MetricsRepository](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/metrics_repository_example.md). Once we do that, we can run anomaly checks that compare the current value of the metric to its values in the past and allow us to detect anomalous changes.

In this simple example, we assume that we compute the size of a dataset every day and we want to ensure that it does not change drastically: the number of rows on a given day should not be more than double of what we have seen on the day before.  

Anomaly detection operates on metrics stored in a metrics repository, so lets create one.
```scala
val metricsRepository = new InMemoryMetricsRepository()
```

This is our fictitious data from yesterday, which only has only two rows. 
```scala
val yesterdaysDataset = itemsAsDataframe(session,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0))   
```

We test for anomalies in the size of the data, it should not increase by more than 2x. We define a check for this by using the [RateOfChangeStrategy](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/anomalydetection/RateOfChangeStrategy.scala) for detecting anomalies. Note that we store the resulting metrics in our repository via `useRepository` and `saveOrAppendResult` under a result key `yesterdaysKey` with yesterdays timestamp.
```scala
val yesterdaysKey = ResultKey(System.currentTimeMillis() - 24 * 60 * 1000) 

VerificationSuite()
  .onData(yesterdaysDataset)
  .useRepository(metricsRepository)
  .saveOrAppendResult(yesterdaysKey)
  .addAnomalyCheck(
    RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
    Size())
  .run()
```  

The fictitions data of todays has five rows, so the data size more than doubled and our anomaly check should
catch this.
```scala
val todaysDataset = itemsAsDataframe(session,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0),
  Item(3, null, null, "low", 5),
  Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
  Item(5, "Thingy E", null, "high", 12))
```
We repeat the anomaly check using our metrics repository.
```scala
val todaysKey = ResultKey(System.currentTimeMillis())

val verificationResult = VerificationSuite()
  .onData(todaysDataset)
  .useRepository(metricsRepository)
  .saveOrAppendResult(todaysKey)
  .addAnomalyCheck(
    RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
    Size())
  .run()
```

We can now have a look at the `status` of the result of the verification to see if your check caught an anomaly (it should have). We print the contents of our metrics repository in that case. 
```scala
if (verificationResult.status != Success) {
  println("Anomaly detected in the Size() metric!")
  
  metricsRepository
    .load()
    .forAnalyzers(Seq(Size()))
    .getSuccessMetricsAsDataFrame(session)
    .show()
}
```

We see that the following metrics are stored in the repository, which shows us the reason the anomaly: the data size increased from 2 to 5!
```
+-------+--------+----+-----+-------------+
| entity|instance|name|value| dataset_date|
+-------+--------+----+-----+-------------+
|Dataset|       *|Size|  2.0|1538384009558|
|Dataset|       *|Size|  5.0|1538385453983|
+-------+--------+----+-----+-------------+
```

Executable, more strategies available, easy to implement your own or connect it to existing libraries
