/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.examples

import com.amazon.deequ.examples.ExampleUtils.withSpark
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}

private[examples] object ConstraintSuggestionExtendedExample extends App {

  withSpark { session =>
    /**
     *  For this case:
     *  Generates 300 rows where at least 75% of the rows are going to be in IN_TRANSIT,
     *  and the 25% left are going to be random among IN_TRANSIT, DELAYED and UNKNOWN.
     *  At least 50% are going the be valuable 'true' and the other 50% random values
     *  among 'true', 'false' and 'null'. ~Adjust as you like
     *  Note:
     *  Send 0 to either statusDist or valuableDist to generate whole set randomly i.e.:
     *  DataProfile(300, 0, 0) so it would generate 300 rows with random statuses and
     *  random valuables.
     */
    val dataProfile = DataProfile(300, 75, 50)
    val generatedData = GenerateDataUtils.generateData(dataProfile)
    val generatedDataRdd = session.sparkContext.parallelize(generatedData)
    val rows = session.createDataFrame(generatedDataRdd)

    // We ask deequ to compute constraint suggestions for us on the data
    // It will profile the data and than apply a set of rules specified in addConstraintRules()
    // to suggest constraints
    val suggestionResult = ConstraintSuggestionRunner()
      .onData(rows)
      .addConstraintRules(Rules.DEFAULT)
      .run()

    // We can now investigate the constraints that deequ suggested. We get a textual description
    // and the corresponding scala code for each suggested constraint
    //
    // Note that the constraint suggestion is based on heuristic rules and assumes that the data it
    // is shown is 'static' and correct, which might often not be the case in the real world.
    // Therefore the suggestions should always be manually reviewed before being applied in real
    // deployments.
    suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
      suggestions.foreach { suggestion =>
        println(s"Constraint suggestion for '$column':\t${suggestion.description}\n" +
          s"The corresponding scala code is ${suggestion.codeForConstraint}\n")
      }
    }

  }

}
