# Anomaly detection

/* In this simple example, we assume that we compute metrics on a dataset every day and we want
to ensure that they don't change drastically. For sake of simplicity, we just look at the
size of the data */

/* Anomaly detection operates on metrics stored in a metric repository, so lets create one */
```scala
val metricsRepository = new InMemoryMetricsRepository()
```

/* This is the key which we use to store the metrics for the dataset from yesterday */
```scala
val yesterdaysKey = ResultKey(System.currentTimeMillis() - 24 * 60 * 1000)
```

/* Yesterday, the data had only two rows */
```scala
val yesterdaysDataset = itemsAsDataframe(session,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0))
```

/* We test for anomalies in the size of the data, it should not increase by more than 2x. Note
that we store the resulting metrics in our repository */
```scala
VerificationSuite()
  .onData(yesterdaysDataset)
  .useRepository(metricsRepository)
  .saveOrAppendResult(yesterdaysKey)
  .addAnomalyCheck(
    RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
    Size())
  .run()
```  

/* Todays data has five rows, so the data size more than doubled and our anomaly check should
catch this */
```scala
val todaysDataset = itemsAsDataframe(session,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0),
  Item(3, null, null, "low", 5),
  Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
  Item(5, "Thingy E", null, "high", 12))
```

/* The key for today's result */
/* Repeat the anomaly check for today's data */
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

/* Did we find an anomaly? */ /* Lets have a look at the actual metrics. */
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

```
+-------+--------+----+-----+-------------+
| entity|instance|name|value| dataset_date|
+-------+--------+----+-----+-------------+
|Dataset|       *|Size|  2.0|1538384009558|
|Dataset|       *|Size|  5.0|1538385453983|
+-------+--------+----+-----+-------------+
```
