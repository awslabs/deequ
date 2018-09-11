## Deequ

Deequ is a library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets.

## Example

```scala
case class Item(id: Long, name: String, description: String, priority: String, numViews: Long)

val rdd = sc.parallelize(Seq(
  Item(1, "Thingy A", "awesome thing.", "high", 0),
  Item(2, "Thingy B", "available at http://thingb.com", null, 0),
  Item(3, null, null, "low", 5),
  Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
  Item(5, "Thingy E", null, "high", 12)))

val data = session.createDataFrame(rdd)
```

```scala
val verificationResult = VerificationSuite()
  .onData(data)
  .addCheck(
    Check(CheckLevel.Error, "unit testing my data") 
      .isComplete("id") // 'id' should never be NULL
      .isUnique("id") // 'id' should not contain duplicates
      .isComplete("name") // 'name' should never be NULL
      // 'priority' should only contain the values "high" and "low"
      .isContainedIn("priority", Array("high", "low")) 
      .isNonNegative("numViews")) // 'numViews' should not contain negative values
      // at least half of the 'description's should contain a url          
      .containsURL("description", _ >= 0.5) 
      // half of the items should have less than 10 'numViews'
      .hasApproxQuantile("numViews", 0.5, _ <= 10)) 
    .run()
```

```scala
if (verificationResult.status == Success) {
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

```
We found errors in the data:

CompletenessConstraint(Completeness(name)): Value: 0.8 does not meet the requirement!
PatternConstraint(containsURL(description)): Value: 0.4 does not meet the requirement!
```

## Advanced features
 
Anomaly detection, incremental tests on growing data, data profiling, constraint suggestion, ... 

## License

This library is licensed under the Apache 2.0 License. 
