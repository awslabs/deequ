# Automatic Suggestion of constraints

In our experience, a major hurdle in data validation is that someone needs to come up with the actual constraints to apply on the data. This can be very difficult for large, real-world datasets, especially if they are very complex and contain information from a lot of different sources. We build so-called *constraint suggestion* functionality into **deequ** to assist users in finding reasonable constraints for their data.

Our constraint suggestion first [profiles the data](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/data_profiling_example.md) and then applies a set of [heuristic rules](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/suggestions/rules) to suggest constraints. In the following, we give a concrete example on how to have constraints suggested for your data.

Lets first generate some example data:
```scala
case class RawData(
  name: String, 
  count: String, 
  status: String, 
  valuable: String
)

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

Now, we ask deequ to compute constraint suggestions for us on the data. It will profile the data and than apply the set of rules specified in `addConstraintRules()` to suggest constraints.
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

The first suggestions we get are for the `valuable` column. **Deequ** correctly identified that this column is actually a boolean column 'disguised' as string column and therefore suggests a constraint on the boolean datatype. Furthermore, it saw that this column contains some missing values and suggests a constraint that checks that the ratio of missing values should not drop in the future.
```
Constraint suggestion for 'valuable': 'valuable' has type Boolean
The corresponding scala code is .hasDataType("valuable", ConstrainableDataTypes.Boolean)

Constraint suggestion for 'valuable': 'valuable' has less than 62% missing values
The corresponding scala code is .hasCompleteness("valuable", _ >= 0.38, Some("It should be above 0.38!"))
```

Next we look at the `count` column. **Deequ** identified that this column is actually a numeric column 'disguised' as string column and therefore suggests a constraint on a fractional datatype (such as float or double). Furthermore, it saw that this column contains some missing values and suggests a constraint that checks that the ratio of missing values should not drop in the future. Additionally, it suggests that values in this column should always be positive (as it did not see any negative values in the example data), which probably makes a lot of sense for count-like data.
```
Constraint suggestion for 'count': 'count' has type Fractional
The corresponding scala code is .hasDataType("count", ConstrainableDataTypes.Fractional)

Constraint suggestion for 'count': 'count' has less than 47% missing values
The corresponding scala code is .hasCompleteness("count", _ >= 0.53, Some("It should be above 0.53!"))

Constraint suggestion for 'count': 'count' has only positive values
The corresponding scala code is .isPositive("count")
```

```
Constraint suggestion for 'name': 'name' is not null
The corresponding scala code is .isComplete("name")

Constraint suggestion for 'name': 'name' has value range 'thingC', 'thingA', 'thingB', 'thingE', 'thingD'
The corresponding scala code is .isContainedIn("name", Array("thingC", "thingA", "thingB", "thingE", "thingD"))

Constraint suggestion for 'status':	'status' is not null
The corresponding scala code is .isComplete("status")

Constraint suggestion for 'status':	'status' has value range 'DELAYED', 'UNKNOWN', 'IN_TRANSIT'
The corresponding scala code is .isContainedIn("status", Array("DELAYED", "UNKNOWN", "IN_TRANSIT"))
```

Hint [data profiling](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/data_profiling_example.md) `suggestionResult.columnProfiles`

Hint `.useTrainTestSplitWithTestsetRatio(0.1)`
