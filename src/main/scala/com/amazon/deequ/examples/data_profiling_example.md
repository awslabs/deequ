# Single column profiling

Very often we are faced with large, raw datasets and struggle to make sense of the data. A common example might be that we are given a huge CSV file and want to understand and clean the data contained therein. **Deequ** supports single-column profiling of such data and its implementation scales to large datasets with billions of rows. In the following, we showcase the basic usage of this profiling functionality:


Assume we have raw data that is string typed (such as the data you would get from a CSV file). For the sake of simplicity, we use the following toy data in this example:

```scala
case class RawData(productName: String, totalNumber: String, status: String, valuable: String)

val rows = session.sparkContext.parallelize(Seq(
  RawData("thingA", "13.0", "IN_TRANSIT", "true"),
  RawData("thingA", "5", "DELAYED", "false"),
  RawData("thingB", null,  "DELAYED", null),
  RawData("thingC", null, "IN_TRANSIT", "false"),
  RawData("thingD", "1.0",  "DELAYED", "true"),
  RawData("thingC", "7.0", "UNKNOWN", null),
  RawData("thingC", "20", "UNKNOWN", null),
  RawData("thingE", "20", "DELAYED", "false")
))

val rawData = session.createDataFrame(rows)
```

It only takes a single method invocation to make **deequ** profile this data. Note that it will execute the three passes over the data and avoid any shuffles in order to easily scale to large data.
```scala
val result = ColumnProfilerRunner()
  .onData(rawData)
  .run()
```

As a result, we get a profile for each column in the data, which allows us to inspect the completeness of the column,
the approximate number of distinct values and the inferred datatype:

```scala
result.profiles.foreach { case (colName, profile) =>
  println(s"Column '$colName':\n " +
    s"\tcompleteness: ${profile.completeness}\n" +
    s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
    s"\tdatatype: ${profile.dataType}\n")
}
```

In case of our toy data, we would get the following profiling results. Note that **deequ** detected that `totalNumber` is a fractional column (and could be casted to float or double type) and that `valuable` is a boolean column.

```
Column 'productName':
 	completeness: 1.0
	approximate number of distinct values: 5
	datatype: String

Column 'totalNumber':
 	completeness: 0.75
	approximate number of distinct values: 5
	datatype: Fractional

Column 'status':
 	completeness: 1.0
	approximate number of distinct values: 3
	datatype: String

Column 'valuable':
 	completeness: 0.625
	approximate number of distinct values: 2
	datatype: Boolean
```

For numeric columns, we get an extended profile which also contains descriptive statistics:
```scala
val totalNumberProfile = result.profiles("totalNumber").asInstanceOf[NumericColumnProfile]

println(s"Statistics of 'totalNumber':\n" +
  s"\tminimum: ${totalNumberProfile.minimum.get}\n" +
  s"\tmaximum: ${totalNumberProfile.maximum.get}\n" +
  s"\tmean: ${totalNumberProfile.mean.get}\n" +
  s"\tstandard deviation: ${totalNumberProfile.stdDev.get}\n")
```

For the `totalNumber` column we can inspect its minimum, maximum, mean and standard deviation:
```
Statistics of 'totalNumber':
	minimum: 1.0
	maximum: 20.0
	mean: 8.25
	standard deviation: 7.280109889280518
```

For columns with a low number of distinct values, we collect the full value distribution.
```scala
val statusProfile = result.profiles("status")

println("Value distribution in 'status':")
statusProfile.histogram.foreach {
  _.values.foreach { case (key, entry) =>
    println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
  }
}
```
Here are accurate statistics about the values in the `status` column:
```
Value distribution in 'status':
	IN_TRANSIT occurred 2 times (ratio is 0.25)
	UNKNOWN occurred 2 times (ratio is 0.25)
	DELAYED occurred 4 times (ratio is 0.5)
```

An [executable version of this example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/DataProfilingExample.scala) is part of our code base.
