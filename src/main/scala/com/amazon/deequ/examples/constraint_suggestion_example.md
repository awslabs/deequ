

Constraint suggestion for 'valuable':	'valuable' has less than 62% missing values
The corresponding scala code is .hasCompleteness("valuable", _ >= 0.38, Some("It should be above 0.38!"))

Constraint suggestion for 'valuable':	'valuable' has type Boolean
The corresponding scala code is .hasDataType("valuable", ConstrainableDataTypes.Boolean)

Constraint suggestion for 'count':	'count' has less than 47% missing values
The corresponding scala code is .hasCompleteness("count", _ >= 0.53, Some("It should be above 0.53!"))

Constraint suggestion for 'count':	'count' has type Fractional
The corresponding scala code is .hasDataType("count", ConstrainableDataTypes.Fractional)

Constraint suggestion for 'count':	'count' has only positive values
The corresponding scala code is .isPositive("count")

Constraint suggestion for 'name':	'name' is not null
The corresponding scala code is .isComplete("name")

Constraint suggestion for 'name':	'name' has value range 'thingC', 'thingA', 'thingB', 'thingE', 'thingD'
The corresponding scala code is .isContainedIn("name", Array("thingC", "thingA", "thingB", "thingE", "thingD"))

Constraint suggestion for 'status':	'status' is not null
The corresponding scala code is .isComplete("status")

Constraint suggestion for 'status':	'status' has value range 'DELAYED', 'UNKNOWN', 'IN_TRANSIT'
The corresponding scala code is .isContainedIn("status", Array("DELAYED", "UNKNOWN", "IN_TRANSIT"))

    // Hint ->
    //suggestionResult.columnProfiles

    // Hint ->
    //       .useTrainTestSplitWithTestsetRatio(0.1)