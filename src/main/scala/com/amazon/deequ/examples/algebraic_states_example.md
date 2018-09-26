# Stateful Metrics Computation

### Incremental metrics computation on growing datasets

```scala
val data = ExampleUtils.itemsAsDataframe(spark,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available tomorrow", "low", 0),
  Item(3, "Thing C", null, null, 5))
```

```scala
val analysis = Analysis()
  .addAnalyzer(Size())
  .addAnalyzer(ApproxCountDistinct("id"))
  .addAnalyzer(Completeness("name"))
  .addAnalyzer(Completeness("description"))
```

// persist the internal state of the computation
```scala
val stateStore = InMemoryStateProvider()

val metricsForData = AnalysisRunner.run(
  data = data,
  analysis = analysis,
  saveStatesWith = Some(stateStore)) 
```


```scala
val moreData = ExampleUtils.itemsAsDataframe(spark,
  Item(4, "Thingy D", null, "low", 10),
  Item(5, "Thingy E", null, "high", 12))
```

// continue from internal state of the computation
```scala
// We update the metrics now from the stored states without having to access the previous data!
val metricsAfterAddingMoreData = AnalysisRunner.run(
  data = moreData,
  analysis = analysis,
  aggregateWith = Some(stateStore) 
)
```

```scala
println("Metrics for the first 3 records:\n")
metricsForData.metricMap.foreach { case (analyzer, metric) =>
  println(s"\t$analyzer: ${metric.value.get}")
}
```

```
Size(None): 3.0
ApproxCountDistinct(id,None): 3.0
Completeness(name,None): 1.0
Completeness(description,None): 0.6666666666666666
```

```scala
println("\nMetrics after adding 2 more records:\n")
metricsAfterAddingMoreData.metricMap.foreach { case (analyzer, metric) =>
  println(s"\t$analyzer: ${metric.value.get}")
}
```

```
Size(None): 5.0
ApproxCountDistinct(id,None): 5.0
Completeness(name,None): 1.0
Completeness(description,None): 0.4
```
An [executable version of this example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/IncrementalMetricsExample.scala) 
is part of our codebase.

