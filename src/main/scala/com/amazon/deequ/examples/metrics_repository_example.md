# Storing Computed Metrics in a MetricsRepository

The toy data on which we will compute metrics

```scala
val data = ExampleUtils.itemsAsDataframe(spark,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0),
  Item(3, null, null, "low", 5),
  Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
  Item(5, "Thingy E", null, "high", 12))
```

A json file in which the computed metrics will be stored, the repository which we will use to stored and load computed metrics; we use the local disk, but it also supports HDFS and S3


```scala
val metricsFile = new File(Files.createTempDir(), "metrics.json")
val repository = FileSystemMetricsRepository(spark, metricsFile.getAbsolutePath)
```

The key under which we store the results, needs a timestamp and supports arbitrary
tags in the form of key-value pairs

```scala
val resultKey = ResultKey(
  System.currentTimeMillis(), 
  Map("tag" -> "repositoryExample"))

VerificationSuite()
  .onData(data)
  .addCheck(Check(CheckLevel.Error, "integrity checks")
    .hasSize(_ == 5)
    .isComplete("id")
    .isComplete("name")
    .isContainedIn("priority", Array("high", "low"))
    .isNonNegative("numViews"))
  .useRepository(repository)
  .saveOrAppendResult(resultKey)
  .run()
```

We can now retrieve the metrics from the repository in different ways, e.g. we can load the metric for a 
particular analyzer stored under our result key:

```scala
val completenessOfName = repository
.loadByKey(resultKey).get
.metric(Completeness("name")).get

println(s"The completeness of the name column is: $completenessOfName")
```
Will output `The completeness of the name column is: DoubleMetric(Column,Completeness,name,Success(0.8))`

We can query the repository for all metrics from the last 10 minutes and get them as json

```scala
val json = repository.load()
  .after(System.currentTimeMillis() - 10000)
  .getSuccessMetricsAsJson()

println(json)
```

```json
[{"name":"Compliance",
  "tag":"repositoryExample",
  "dataset_date":1537951323402,
  "instance":"numViews is non-negative",
  "entity":"Column",
  "value":1.0},
 {"name":"Compliance",
  "tag":"repositoryExample",
  "dataset_date":1537951323402,
  "instance":"priority contained in high,low",
  "entity":"Column",
  "value":1.0},
 {"name":"Size",
  "tag":"repositoryExample",
  "dataset_date":1537951323402,
  "instance":"*",
  "entity":"Dataset",
  "value":5.0},
 {"name":"Completeness",
  "tag":"repositoryExample",
  "dataset_date":1537951323402,
  "instance":"id",
  "entity":"Column",
  "value":1.0},
 {"name":"Completeness",
  "tag":"repositoryExample",
  "dataset_date":1537951323402,
  "instance":"name",
  "entity":"Column",
  "value":0.8}]
```

Finally we can also query by tag value and retrieve the result in the form of a dataframe

```scala
repository.load()
  .withTagValues(Map("tag" -> "repositoryExample"))
  .getSuccessMetricsAsDataFrame(spark)
  .show()
```

```
+-------+--------------------+------------+-----+-------------+-----------------+
| entity|            instance|        name|value| dataset_date|              tag|
+-------+--------------------+------------+-----+-------------+-----------------+
| Column|numViews is Fnon-...|  Compliance|  1.0|1537951323402|repositoryExample|
| Column|priority containe...|  Compliance|  1.0|1537951323402|repositoryExample|
|Dataset|                   *|        Size|  5.0|1537951323402|repositoryExample|
| Column|                  id|Completeness|  1.0|1537951323402|repositoryExample|
| Column|                name|Completeness|  0.8|1537951323402|repositoryExample|
+-------+--------------------+------------+-----+-------------+-----------------+
```

