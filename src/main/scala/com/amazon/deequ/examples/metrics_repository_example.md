# Storing Computed Metrics in a MetricsRepository

**Deequ** allows us to persist the metrics we computed on dataframes in a so-called [MetricsRepository](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/repository/MetricsRepository.scala). In the following example, we showcase how to store metrics in a filesystem and query them later on.

Let's start by creating some toy data on which we will compute metrics:

```scala
val data = ExampleUtils.itemsAsDataframe(spark,
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0),
  Item(3, null, null, "low", 5),
  Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
  Item(5, "Thingy E", null, "high", 12))
```

Next, we setup a repository. In this example, we use a [FileSystemMetricsRepository](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/repository/fs/FileSystemMetricsRepository.scala) which allows us to store the metrics in json format on the local disk (note that it also supports HDFS and S3).

```scala
val metricsFile = new File(Files.createTempDir(), "metrics.json")
val repository = FileSystemMetricsRepository(spark, metricsFile.getAbsolutePath)
```
Each set of metrics that we computed needs be indexed by a so-called [ResultKey](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/repository/MetricsRepository.scala), which contains a timestamp and supports arbitrary
tags in the form of key-value pairs. Let's setup one for this example:

```scala
val resultKey = ResultKey(
  System.currentTimeMillis(),
  Map("tag" -> "repositoryExample"))
```

Now we can run checks on our data as usual. However, we make deequ store the resulting metrics for the checks in our repository by adding the `useRepository` and `saveOrAppendResult` methods to our invocation:

```scala
VerificationSuite()
  .onData(data)
  .addCheck(Check(CheckLevel.Error, "integrity checks")
    .hasSize(_ == 5)
    .isComplete("id")
    .isComplete("productName")
    .isContainedIn("priority", Array("high", "low"))
    .isNonNegative("numViews"))
  .useRepository(repository)
  .saveOrAppendResult(resultKey)
  .run()
```

**Deequ** now executes the verification as usual and additionally stores the metrics under our specified key. Afterwards, we can retrieve the metrics from the repository in different ways. We can for example directly load the metric for a
particular analyzer stored under our result key as follows:

```scala
val completenessOfProductName = repository
  .loadByKey(resultKey).get
  .metric(Completeness("productName")).get

println(s"The completeness of the productName column is: $completenessOfProductName")
```

Executing this code will output `The completeness of the productName column is: DoubleMetric(Column,Completeness,Name,Success(0.8))`.

All our repositories support a couple of more general querying methods, e.g., we can also ask the repository for all metrics from the last 10 minutes and have it return the output as json:

```scala
val json = repository.load()
  .after(System.currentTimeMillis() - 10000)
  .getSuccessMetricsAsJson()

println(json)
```
This will show us the json representation of the metrics we computed so far:

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
  "instance":"productName",
  "entity":"Column",
  "value":0.8}]
```

Additionally, we can also query by tag value and retrieve the result in the form of a spark dataframe:

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
| Column|         productName|Completeness|  0.8|1537951323402|repositoryExample|
+-------+--------------------+------------+-----+-------------+-----------------+
```

We provide an [executable version of this example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/MetricsRepositoryExample.scala) as part of our codebase.
