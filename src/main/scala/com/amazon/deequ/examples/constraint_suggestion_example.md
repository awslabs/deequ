# Automatic Suggestion of constraints

Lets first generate some example data
```scala
val rows = session.sparkContext.parallelize(Seq(
  RawData("thingA", "13.0", "IN_TRANSIT", "true"),
  RawData("thingA", "5", "DELAYED", "false"),
  RawData("thingB", null, "DELAYED", null),
  RawData("thingC", null, "IN_TRANSIT", "false"),
  RawData("thingD", "1.0", "DELAYED", "true"),
  RawData("thingC", "7.0", "UNKNOWN", null),
  RawData("thingC", "24", "UNKNOWN", null),
  RawData("thingE", "20", "DELAYED", "false"),
  RawData("thingA", "13.0", "IN_TRANSIT", "true"),
  RawData("thingA", "5", "DELAYED", "false"),
  RawData("thingB", null, "DELAYED", null),
  RawData("thingC", null, "IN_TRANSIT", "false"),
  RawData("thingD", "1.0", "DELAYED", "true"),
  RawData("thingC", "17.0", "UNKNOWN", null),
  RawData("thingC", "22", "UNKNOWN", null),
  RawData("thingE", "23", "DELAYED", "false")
))

val data = session.createDataFrame(rows)
```

We ask deequ to compute constraint suggestions for us on the data. It will profile the data and than apply a set of rules specified in `addConstraintRules()` to suggest constraints
```scala
val suggestionResult = ConstraintSuggestionRunner()
  .onData(data)
  .addConstraintRules(Rules.DEFAULT)
  .run()
```

We can now investigate the constraints that deequ suggested. We get a textual description and the corresponding scala code for each suggested constraint. Note that the constraint suggestion is based on heuristic rules and assumes that the data it is shown is 'static' and correct, which might often not be the case in the real world. Therefore the suggestions should always be manually reviewed before being applied in real deployments.
```scala
suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
  suggestions.foreach { suggestion =>
    println(s"Constraint suggestion for '$column':\t${suggestion.description}\n" +
      s"The corresponding scala code is ${suggestion.codeForConstraint}\n")
  }
}
```

```
Constraint suggestion for 'valuable': 'valuable' has less than 62% missing values
The corresponding scala code is .hasCompleteness("valuable", _ >= 0.38, Some("It should be above 0.38!"))

Constraint suggestion for 'valuable': 'valuable' has type Boolean
The corresponding scala code is .hasDataType("valuable", ConstrainableDataTypes.Boolean)
```

```
Constraint suggestion for 'count': 'count' has less than 47% missing values
The corresponding scala code is .hasCompleteness("count", _ >= 0.53, Some("It should be above 0.53!"))

Constraint suggestion for 'count': 'count' has type Fractional
The corresponding scala code is .hasDataType("count", ConstrainableDataTypes.Fractional)

Constraint suggestion for 'count': 'count' has only positive values
The corresponding scala code is .isPositive("count")
```

```
Constraint suggestion for 'name': 'name' is not null
The corresponding scala code is .isComplete("name")

Constraint suggestion for 'name': 'name' has value range 'thingC', 'thingA', 'thingB', 'thingE', 'thingD'
The corresponding scala code is .isContainedIn("name", Array("thingC", "thingA", "thingB", "thingE", "thingD"))
```

```
Constraint suggestion for 'status':	'status' is not null
The corresponding scala code is .isComplete("status")

Constraint suggestion for 'status':	'status' has value range 'DELAYED', 'UNKNOWN', 'IN_TRANSIT'
The corresponding scala code is .isContainedIn("status", Array("DELAYED", "UNKNOWN", "IN_TRANSIT"))
```

Hint [data profiling](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/data_profiling_example.md) `suggestionResult.columnProfiles`

Hint `.useTrainTestSplitWithTestsetRatio(0.1)`
