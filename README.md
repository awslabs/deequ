# Deequ - Unit Tests for Data
[![GitHub license](https://img.shields.io/github/license/awslabs/deequ.svg)](https://github.com/awslabs/deequ/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/awslabs/deequ.svg)](https://github.com/awslabs/deequ/issues)
[![Build Status](https://github.com/awslabs/deequ/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/awslabs/deequ/actions/workflows/maven.yml?query=branch%3Amaster)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.amazon.deequ/deequ/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.amazon.deequ/deequ)

Deequ is a library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets. We are happy to receive feedback and [contributions](CONTRIBUTING.md).

Python users may also be interested in PyDeequ, a Python interface for Deequ. You can find PyDeequ on [GitHub](https://github.com/awslabs/python-deequ), [readthedocs](https://pydeequ.readthedocs.io/en/latest/README.html), and [PyPI](https://pypi.org/project/pydeequ/).

## Requirements and Installation

__Deequ__ depends on Java 8 or higher. The current version runs with Apache Spark 3.5.6 and Scala 2.13. For compatibility with previous versions, please refer to the compatibility matrix below.

### Compatibility Matrix

| Deequ Version | Scala Version | Spark Version | Java Version |
|---------------|---------------|---------------|--------------|
| 3.0.x         | 2.13.15       | 3.5.6         | 8+           |
| 2.0.x         | 2.12.x        | 3.1.x         | 8+           |
| 1.x           | 2.11/2.12     | 2.2.x-3.0.x   | 8+           |

Available via [maven central](http://mvnrepository.com/artifact/com.amazon.deequ/deequ). 

Choose the latest release that matches your Spark and Scala versions from the [available versions](https://repo1.maven.org/maven2/com/amazon/deequ/deequ/). Add the release as a dependency to your project.

### Maven Dependencies

For Scala 2.13 and Spark 3.5.6:

```xml
<dependency>
  <groupId>com.amazon.deequ</groupId>
  <artifactId>deequ_2.13</artifactId>
  <version>3.0.0-spark-3.5</version>
</dependency>
```

### SBT Dependencies

For Scala 2.13 and Spark 3.5.6:

```scala
libraryDependencies += "com.amazon.deequ" %% "deequ" % "3.0.0-spark-3.5"
```

### Legacy Versions

If you need to use previous Spark versions, please use the appropriate legacy version:

- **Spark 3.1.x with Scala 2.12**: `"com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1"`
- **Spark 2.4.x-3.0.x with Scala 2.12**: Use Deequ 1.x versions
- **Spark 2.2.x-2.3.x with Scala 2.11**: Use Deequ 1.x versions

### Installation Instructions

#### Prerequisites

- **Java**: Version 8 or higher
- **Scala**: Version 2.13.15 (for current version)
- **Apache Spark**: Version 3.5.6 (for current version)

#### Maven Setup

Add the following to your `pom.xml`:

```xml
<properties>
    <scala.major.version>2.13</scala.major.version>
    <scala.version>2.13.15</scala.version>
    <spark.version>3.5.6</spark.version>
</properties>

<dependencies>
    <!-- Deequ dependency -->
    <dependency>
        <groupId>com.amazon.deequ</groupId>
        <artifactId>deequ_2.13</artifactId>
        <version>3.0.0-spark-3.5</version>
    </dependency>
    
    <!-- Spark dependencies (if not already included) -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.13</artifactId>
        <version>3.5.6</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.13</artifactId>
        <version>3.5.6</version>
    </dependency>
</dependencies>
```

#### SBT Setup

Add the following to your `build.sbt`:

```scala
scalaVersion := "2.13.15"

val sparkVersion = "3.5.6"

libraryDependencies ++= Seq(
  "com.amazon.deequ" %% "deequ" % "3.0.0-spark-3.5",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
```

#### Gradle Setup

Add the following to your `build.gradle`:

```gradle
dependencies {
    implementation 'com.amazon.deequ:deequ_2.13:3.0.0-spark-3.5'
    implementation 'org.apache.spark:spark-core_2.13:3.5.6'
    implementation 'org.apache.spark:spark-sql_2.13:3.5.6'
}
```

### Breaking Changes and Migration Notes

#### Migrating from Deequ 2.x (Scala 2.12)

When upgrading from Deequ 2.x to 3.x, please note the following changes:

1. **Scala Version**: Update your project to use Scala 2.13.15
2. **Spark Version**: Update to Spark 3.5.6
3. **Artifact Name**: Change from `deequ` to `deequ_2.13` in Maven coordinates
4. **Collection Imports**: If you extend Deequ classes, update collection imports:
   - Replace `scala.collection.JavaConverters._` with `scala.jdk.CollectionConverters._`
   - Update collection operations using `breakOut` pattern to use `.to(Map)` or similar

#### Known Issues

- Custom analyzers extending Deequ classes may need minor updates for Scala 2.13 compatibility
- Serialized state from previous versions may not be compatible (regenerate metrics repositories)

For detailed migration guidance, see the [Migration Guide](MIGRATION_GUIDE.md).

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

Most applications that work with data have implicit assumptions about that data, e.g., that attributes have certain types, do not contain NULL values, and so on. If these assumptions are violated, your application might crash or produce wrong outputs. The idea behind __deequ__ is to explicitly state these assumptions in the form of a "unit-test" for data, which can be verified on a piece of data at hand. If the data has errors, we can "quarantine" and fix it, before we feed it to an application.

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
The test found that our assumptions are violated! Only 4 out of 5 (80%) of the values of the `productName` attribute are non-null and only 2 out of 5 (40%) values of the `description` attribute did contain a url. Fortunately, we ran a test and found the errors, somebody should immediately fix the data :)

## More examples

Our library contains much more functionality than what we showed in the basic example. We are in the process of adding [more examples](src/main/scala/com/amazon/deequ/examples/) for its advanced features. So far, we showcase the following functionality:

 * [Persistence and querying of computed metrics of the data with a MetricsRepository](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/metrics_repository_example.md)
 * [Data profiling](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/data_profiling_example.md) of large data sets
 * [Anomaly detection](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/anomaly_detection_example.md) on data quality metrics over time
 * [Automatic suggestion of constraints](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/constraint_suggestion_example.md) for large datasets
 * [Incremental metrics computation on growing data and metric updates on partitioned data](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/algebraic_states_example.md) (advanced)

## DQDL (Data Quality Definition Language)

Deequ also supports [DQDL](https://docs.aws.amazon.com/glue/latest/dg/dqdl.html), a declarative language for defining data quality rules. DQDL allows you to express data quality constraints in a simple, readable format.

### Supported DQDL Rules

- **RowCount**: `RowCount < 100`
- **Completeness**: `Completeness "column" > 0.9`
- **IsComplete**: `IsComplete "column"`
- **Uniqueness**: `Uniqueness "column" = 1.0`
- **IsUnique**: `IsUnique "column"`
- **ColumnCorrelation**: `ColumnCorrelation "col1" "col2" > 0.8`
- **DistinctValuesCount**: `DistinctValuesCount "column" = 5`
- **Entropy**: `Entropy "column" > 2.0`
- **Mean**: `Mean "column" between 10 and 50`
- **StandardDeviation**: `StandardDeviation "column" < 5.0`
- **Sum**: `Sum "column" = 100`
- **UniqueValueRatio**: `UniqueValueRatio "column" > 0.7`
- **CustomSql**: `CustomSql "SELECT COUNT(*) FROM primary" > 0`
- **IsPrimaryKey**: `IsPrimaryKey "column"`
- **ColumnLength**: `ColumnLength "column" between 1 and 5`
- **ColumnExists**: `ColumnExists "column"`

### Scala Example

ScalaDQDLExample.scala

```scala
import com.amazon.deequ.dqdl.EvaluateDataQuality
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("DQDL Example")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Sample data
val df = Seq(
  ("1", "a", "c"),
  ("2", "a", "c"),
  ("3", "a", "c"),
  ("4", "b", "d")
).toDF("item", "att1", "att2")

// Define rules using DQDL syntax
val ruleset = """Rules=[IsUnique "item", RowCount < 10, Completeness "item" > 0.8, Uniqueness "item" = 1.0]"""

// Evaluate data quality
val results = EvaluateDataQuality.process(df, ruleset)
results.show()
```

### Java Example

JavaDQDLExample.java

```java
import com.amazon.deequ.dqdl.EvaluateDataQuality;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

SparkSession spark = SparkSession.builder()
    .appName("DQDL Java Example")
    .master("local[*]")
    .getOrCreate();

// Create sample data
Dataset<Row> df = spark.sql(
    "SELECT * FROM VALUES " +
    "('1', 'a', 'c'), " +
    "('2', 'a', 'c'), " +
    "('3', 'a', 'c'), " +
    "('4', 'b', 'd') " +
    "AS t(item, att1, att2)"
);

// Define rules using DQDL syntax
String ruleset = "Rules=[IsUnique \"item\", RowCount < 10, Completeness \"item\" > 0.8, Uniqueness \"item\" = 1.0]";

// Evaluate data quality
Dataset<Row> results = EvaluateDataQuality.process(df, ruleset);
results.show();
```

## Citation

If you would like to reference this package in a research paper, please cite:

Sebastian Schelter, Dustin Lange, Philipp Schmidt, Meltem Celikel, Felix Biessmann, and Andreas Grafberger. 2018. [Automating large-scale data quality verification](http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf). Proc. VLDB Endow. 11, 12 (August 2018), 1781-1794.

## License

This library is licensed under the Apache 2.0 License.
