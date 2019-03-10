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

import org.scalatest.WordSpec

class ExamplesTest extends WordSpec {

  "all examples" should {
    "run without errors" in {
      BasicExample.main(Array.empty)
      IncrementalMetricsExample.main(Array.empty)
      MetricsRepositoryExample.main(Array.empty)
      UpdateMetricsOnPartitionedDataExample.main(Array.empty)
      DataProfilingExample.main(Array.empty)
      AnomalyDetectionExample.main(Array.empty)
      ConstraintSuggestionExample.main(Array.empty)
    }
  }

}
