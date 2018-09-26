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
An [executable version of this example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/IncrementalMetricsExample.scala) is part of our codebase.

## Update metrics on partitioned data

// Assume we store and process our data in a partitioned manner:
// In this example, we operate on a table of manufacturers partitioned by country code

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

```scala
// We are interested in the the following constraints of the table as a whole
val check = Check(CheckLevel.Warning, "a check")
  .isComplete("name")
  .containsURL("name", _ == 0.0)
  .isContainedIn("countryCode", Array("DE", "US", "CN"))
```
// Deequ now allows us to compute states for the metrics on which the constraints are defined
// according to the partitions of the data.

// We first compute and store the state per partition
// Next, we compute the metrics for the whole table from the partition states
// Note that we do not need to touch the data again, the states are sufficient
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

// Lets now assume that a single partition changes. We only need to recompute the state of this
// partition in order to update the metrics for the whole table.

```scala
val updatedUsManufacturers = ExampleUtils.manufacturersAsDataframe(spark,
  Manufacturer(3, "ManufacturerDNew", "US"),
  Manufacturer(4, null, "US"),
  Manufacturer(5, "ManufacturerFNew http://clickme.com", "US"))
```

// Recompute state of partition
// Recompute metrics for whole tables from states. We do not need to touch old data!
```scala
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
