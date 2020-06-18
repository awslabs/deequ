# Deequ - Unit Tests for Data
[![GitHub license](https://img.shields.io/github/license/awslabs/deequ.svg)](https://github.com/awslabs/deequ/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/awslabs/deequ.svg)](https://github.com/awslabs/deequ/issues)
[![Build Status](https://travis-ci.org/awslabs/deequ.svg?branch=master)](https://travis-ci.org/awslabs/deequ)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.amazon.deequ/deequ/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.amazon.deequ/deequ)

Deequ is a library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets. We are happy to receive feedback and [contributions](CONTRIBUTING.md).

## Requirements and Installation

__Deequ__ depends on Java 8 and is known to work with Apache Spark versions 2.2.x to 2.4.x.

Available via [maven central](http://mvnrepository.com/artifact/com.amazon.deequ/deequ). 
Add the latest release as a dependency to your project:
__Maven__
```
<dependency>
  <groupId>com.amazon.deequ</groupId>
  <artifactId>deequ</artifactId>
  <version>1.0.4</version>
</dependency>
```

__sbt__
```
libraryDependencies += "com.amazon.deequ" % "deequ" % "1.0.4"
```

## Example

__Deequ__'s purpose is to "unit-test" data to find errors early, before the data gets fed to consuming systems or machine learning algorithms. In the following, we will walk you through a toy example to showcase the most basic usage of our library. An executable version of the example is available [here](/src/main/scala/com/amazon/deequ/examples/BasicExample.scala).

__Deequ__ works on tabular data, e.g., CSV files, database tables, logs, flattened json files, basically anything that you can fit into a Spark dataframe. For this example, we assume that we work on some kind of `Item` data, where every item has an id, a productName, a description, a priority and a count of how often it has been viewed.

```scala
case class Item(
  id: Long,
  productName: String,
  description: String,
  priority: String,
  numViews: Long
)
```

Our library is built on [Apache Spark](https://spark.apache.org/) and is designed to work with very large datasets (think billions of rows) that typically live in a distributed filesystem or a data warehouse. For the sake of simplicity in this example, we just generate a few toy records though.

```scala
val rdd = spark.sparkContext.parallelize(Seq(
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0),
  Item(3, null, null, "low", 5),
  Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
  Item(5, "Thingy E", null, "high", 12)))

val data = spark.createDataFrame(rdd)
```

Most applications that work with data have implicit assumptions about that data, e.g., that attributes have certain types, do not contain NULL values, and so on. If these assumptions are violated, your application might crash or produce wrong outputs. The idea behind __deequ__ is to explicitly state these assumptions in the form of a "unit-test" for data, which can be verified on a piece of data at hand. If the data has errors, we can "quarantine" and fix it, before we feed to an application.

The main entry point for defining how you expect your data to look is the [VerificationSuite](src/main/scala/com/amazon/deequ/VerificationSuite.scala) from which you can add [Checks](src/main/scala/com/amazon/deequ/checks/Check.scala) that define constraints on attributes of the data. In this example, we test for the following properties of our data:

  * there are 5 rows in total
  * values of the `id` attribute are never NULL and unique
  * values of the `productName` attribute are never NULL
  * the `priority` attribute can only contain "high" or "low" as value
  * `numViews` should not contain negative values
  * at least half of the values in `description` should contain a url
  * the median of `numViews` should be less than or equal to 10

In code this looks as follows:

```scala
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}


val verificationResult = VerificationSuite()
  .onData(data)
  .addCheck(
    Check(CheckLevel.Error, "unit testing my data")
      .hasSize(_ == 5) // we expect 5 rows
      .isComplete("id") // should never be NULL
      .isUnique("id") // should not contain duplicates
      .isComplete("productName") // should never be NULL
      // should only contain the values "high" and "low"
      .isContainedIn("priority", Array("high", "low"))
      .isNonNegative("numViews") // should not contain negative values
      // at least half of the descriptions should contain a url
      .containsURL("description", _ >= 0.5)
      // half of the items should have less than 10 views
      .hasApproxQuantile("numViews", 0.5, _ <= 10))
    .run()
```

After calling `run`, __deequ__ translates your test to a series of Spark jobs, which it executes to compute metrics on the data. Afterwards it invokes your assertion functions (e.g., `_ == 5` for the size check) on these metrics to see if the constraints hold on the data. We can inspect the [VerificationResult](src/main/scala/com/amazon/deequ/VerificationResult.scala) to see if the test found errors:

```scala
import com.amazon.deequ.constraints.ConstraintStatus


if (verificationResult.status == CheckStatus.Success) {
  println("The data passed the test, everything is fine!")
} else {
  println("We found errors in the data:\n")

  val resultsForAllConstraints = verificationResult.checkResults
    .flatMap { case (_, checkResult) => checkResult.constraintResults }

  resultsForAllConstraints
    .filter { _.status != ConstraintStatus.Success }
    .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
}
```

If we run the example, we get the following output:
```
We found errors in the data:

CompletenessConstraint(Completeness(productName)): Value: 0.8 does not meet the requirement!
PatternConstraint(containsURL(description)): Value: 0.4 does not meet the requirement!
```
The test found that our assumptions are violated! Only 4 out of 5 (80%) of the values of the `productName` attribute are non-null and only 2 out of 5 (40%) values of the `description` attribute contained a url. Fortunately, we ran a test and found the errors, somebody should immediately fix the data :)

## More examples

Our library contains much more functionaliy than what we showed in the basic example. We are in the process of adding [more examples](src/main/scala/com/amazon/deequ/examples/) for its advanced features. So far, we showcase the following functionality:

 * [Persistence and querying of computed metrics of the data with a MetricsRepository](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/metrics_repository_example.md)
 * [Data profiling](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/data_profiling_example.md) of large data sets
 * [Anomaly detection](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/anomaly_detection_example.md) on data quality metrics over time
 * [Automatic suggestion of constraints](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/constraint_suggestion_example.md) for large datasets
 * [Incremental metrics computation on growing data and metric updates on partitioned data](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/algebraic_states_example.md) (advanced)


## Citation

If you would like to reference this package in a research paper, please cite:

Sebastian Schelter, Dustin Lange, Philipp Schmidt, Meltem Celikel, Felix Biessmann, and Andreas Grafberger. 2018. [Automating large-scale data quality verification](http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf). Proc. VLDB Endow. 11, 12 (August 2018), 1781-1794.

## License

This library is licensed under the Apache 2.0 License.
