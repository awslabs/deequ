    // We profile raw data, mostly in string format (e.g., from a csv file)
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

    /* Make deequ profile this data. It will execute the three passes over the data and avoid
       any shuffles. */
    val result = ColumnProfilerRunner()
      .onData(rawData)
      .run()

    /* We get a profile for each column which allows to inspect the completeness of the column,
       the approximate number of distinct values and the inferred datatype. */
    result.profiles.foreach { case (name, profile) =>

      println(s"Column '$name':\n " +
        s"\tcompleteness: ${profile.completeness}\n" +
        s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
        s"\tdatatype: ${profile.dataType}\n")
    }

    /* For numeric columns, we get descriptive statistics */
    val countProfile = result.profiles("count").asInstanceOf[NumericColumnProfile]

    println(s"Statistics of 'count':\n" +
      s"\tminimum: ${countProfile.minimum.get}\n" +
      s"\tmaximum: ${countProfile.maximum.get}\n" +
      s"\tmean: ${countProfile.mean.get}\n" +
      s"\tstandard deviation: ${countProfile.stdDev.get}\n")

    val statusProfile = result.profiles("status")

    /* For columns with a low number of distinct values, we get the full value distribution. */
    println("Value distribution in 'stats':")
    statusProfile.histogram.foreach {
      _.values.foreach { case (key, entry) =>
        println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
      }
    }

Column 'name':
 	completeness: 1.0
	approximate number of distinct values: 5
	datatype: String

Column 'count':
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

Statistics of 'count':
	minimum: 1.0
	maximum: 20.0
	mean: 8.25
	standard deviation: 7.280109889280518

Value distribution in 'stats':
	IN_TRANSIT occurred 2 times (ratio is 0.25)
	UNKNOWN occurred 2 times (ratio is 0.25)
	DELAYED occurred 4 times (ratio is 0.5)