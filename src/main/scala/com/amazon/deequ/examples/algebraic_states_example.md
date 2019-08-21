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
  .addAnalyzer(Completeness("productName"))
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

Executing this code will print the following. We see that there are 3 records in the data, the cardinality of the `ìd` column is 3, there are no missing values in the `productName` column and two-thirds of the `description` column have values.

```
Size(None): 3.0
ApproxCountDistinct(id,None): 3.0
Completeness(productName,None): 1.0
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
Executing this code prints the updated metrics for the whole dataset. We have 5 records overall, the cardinality of the `ìd` column is also 5, there are still no missing values in the `productName` column and the completeness of the `description` column dropped to 0.4.

```
Size(None): 5.0
ApproxCountDistinct(id,None): 5.0
Completeness(productName,None): 1.0
Completeness(description,None): 0.4
```
An [executable version of this example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/IncrementalMetricsExample.scala) is part of our codebase.

## Update metrics on partitioned data

In the next application of stateful computation, we process data in a different way. Instead of working on growing data, we assume that we store and update our data in a partitioned manner. In this example, we operate on a table of manufacturers partitioned by country code:

```scala
val deManufacturers = ExampleUtils.manufacturersAsDataframe(spark,
  Manufacturer(1, "ManufacturerA", "DE"),
  Manufacturer(2, "ManufacturerB", "DE"))

val usManufacturers = ExampleUtils.manufacturersAsDataframe(spark,
  Manufacturer(3, "ManufacturerD", "US"),
  Manufacturer(4, "ManufacturerE", "US"),
  Manufacturer(5, "ManufacturerF", "US"))

val cnManufacturers = ExampleUtils.manufacturersAsDataframe(spark,
  Manufacturer(6, "ManufacturerG", "CN"),
  Manufacturer(7, "ManufacturerH", "CN"))
```

And we have the following metrics (defined by a check) that we're interested in for the whole table:

```scala
val check = Check(CheckLevel.Warning, "a check")
  .isComplete("productName")
  .containsURL("productName", _ == 0.0)
  .isContainedIn("countryCode", Array("DE", "US", "CN"))
```

**Deequ** now allows us to compute states for the metrics on which the constraints are defined according to the partitions of the data. We first compute and store the state per partition, and than cheaply compute the metrics for the whole table from the partition states via the `runOnAggregatedStates` method. (Note that we do not need to touch the data again, the states are sufficient).

```scala
val analysis = Analysis(check.requiredAnalyzers().toSeq)

val deStates = InMemoryStateProvider()
val usStates = InMemoryStateProvider()
val cnStates = InMemoryStateProvider()

AnalysisRunner.run(deManufacturers, analysis, saveStatesWith = Some(deStates))
AnalysisRunner.run(usManufacturers, analysis, saveStatesWith = Some(usStates))
AnalysisRunner.run(cnManufacturers, analysis, saveStatesWith = Some(cnStates))

val tableMetrics = AnalysisRunner.runOnAggregatedStates(
  deManufacturers.schema, 
  analysis,
  Seq(deStates, usStates, cnStates)
)

println("Metrics for the whole table:\n")
tableMetrics.metricMap.foreach { case (analyzer, metric) =>
  println(s"\t$analyzer: ${metric.value.get}")
}
```

```
Completeness(productName,None): 1.0
PatternMatch(productName,(https?|ftp)://[^\s/$.?#].[^\s]*,None): 0.0
Compliance("countryCode contained in DE,US,CN", 
  countryCode IS NULL OR countryCode IN ('DE','US','CN'),None): 1.0
```

Let us now assume that a single partition changes and that we need to recompute the metrics for the table as a whole. The advantage provided by the stateful computation is that we only need to recompute the state of the changed partition in order to update the metrics for the whole table. We do not need to touch the other partitions!

```scala
val updatedUsManufacturers = ExampleUtils.manufacturersAsDataframe(spark,
  Manufacturer(3, "ManufacturerDNew", "US"),
  Manufacturer(4, null, "US"),
  Manufacturer(5, "ManufacturerFNew http://clickme.com", "US"))
  
val updatedUsStates = InMemoryStateProvider()

AnalysisRunner.run(
  updatedUsManufacturers, 
  analysis, 
  saveStatesWith = Some(updatedUsStates)
)

val updatedTableMetrics = AnalysisRunner.runOnAggregatedStates(
  deManufacturers.schema, 
  analysis,
  Seq(deStates, updatedUsStates, cnStates)
)

println("Metrics for the whole table after updating the US partition:\n")
updatedTableMetrics.metricMap.foreach { case (analyzer, metric) =>
  println(s"\t$analyzer: ${metric.value.get}")
}
```

This code will only operate on the updated partition and the states, but will still return the correct metrics for the table as a whole:

```
Completeness(productName,None): 0.8571428571428571
PatternMatch(productName,(https?|ftp)://[^\s/$.?#].[^\s]*,None): 0.14285714285714285
Compliance("countryCode contained in DE,US,CN", 
  countryCode IS NULL OR countryCode IN ('DE','US','CN'),None): 1.0
```

An [executable version of this example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/UpdateMetricsOnPartitionedDataExample.scala) is part of our codebase.
