# Stateful Metrics Computation

In the following examples, we provide insights into more advanced features of **deequ**: stateful computation of data quality metrics for data that changes. Note that this functionality is currently not fully integrated into our high-level APIs and might be subject to change in the future.

### Incremental metrics computation on growing datasets

A common scenario in real-world deployments is that we have datasets that grow over time by appending new rows. Often the data stems from log files or systems events. While we are probably very interested in verifying data quality metrics on the dataset after we added new data, we almost surely do not want to have to read the whole data each time to update our quality metrics.

This is the prime use case for **deequ**'s stateful metrics computations. Internally, **deequ** computes states on data partitions which can be aggregated and form the input to its metrics computations. We can use these states to cheaply update metrics for growing datasets as follows:

First, we generate some example data and specify a set of metrics that we want to compute in the form of an [Analysis](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Analysis.scala).

```scala
val data = ExampleUtils.itemsAsDataframe(spark,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available tomorrow", "low", 0),
  Item(3, "Thing C", null, null, 5))

val analysis = Analysis()
  .addAnalyzer(Size())
  .addAnalyzer(ApproxCountDistinct("id"))
  .addAnalyzer(Completeness("name"))
  .addAnalyzer(Completeness("description"))
```
We can now trigger the execution of these analyzers via the [AnalysisRunner](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/runners/AnalysisRunner.scala). However, we want to persist the internal state of the computation; therefore we configure a [StateProvider](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/StateProvider.scala) in order to capture this state.

```scala
val stateStore = InMemoryStateProvider()

val metricsForData = AnalysisRunner.run(
  data = data,
  analysis = analysis,
  saveStatesWith = Some(stateStore)) 
```

We can now inspect the metrics for the current version of the data:

```scala
println("Metrics for the first 3 records:\n")
metricsForData.metricMap.foreach { case (analyzer, metric) =>
  println(s"\t$analyzer: ${metric.value.get}")
}
```

Executing this code will print the following. We see that there are 3 records in the data, the cardinality of the `ìd` column is 3, there are no missing values in the `name` column and two-thirds of the `description` column have values.

```
Size(None): 3.0
ApproxCountDistinct(id,None): 3.0
Completeness(name,None): 1.0
Completeness(description,None): 0.6666666666666666
```

Now lets assume we somehow gathered more data that we want to add to our dataset. We would now like to know the updated metrics for the whole dataset, but we do not want to read the old data again. Fortunately, **deequ** allows us to continue from the internal state of the computation and update the metrics from the stored states without having to access the previous data! 

```scala
val moreData = ExampleUtils.itemsAsDataframe(spark,
  Item(4, "Thingy D", null, "low", 10),
  Item(5, "Thingy E", null, "high", 12))

val metricsAfterAddingMoreData = AnalysisRunner.run(
  data = moreData,
  analysis = analysis,
  aggregateWith = Some(stateStore) 
)

println("\nMetrics after adding 2 more records:\n")
metricsAfterAddingMoreData.metricMap.foreach { case (analyzer, metric) =>
  println(s"\t$analyzer: ${metric.value.get}")
}
```
Executing this code prints the updated metrics for the whole dataset. We have 5 records overall, the cardinality of the `ìd` column is also 5, there are still no missing values in the `name` column and the completeness of the `description` column dropped to 0.4.

```
Size(None): 5.0
ApproxCountDistinct(id,None): 5.0
Completeness(name,None): 1.0
Completeness(description,None): 0.4
```
An [executable version of this example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/IncrementalMetricsExample.scala) 
is part of our codebase.

