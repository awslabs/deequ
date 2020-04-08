/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  * http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.checks

import org.scalatest.WordSpec

class ColumnConditionTest extends WordSpec {

  "ColumnCondition" should {

    "return the correct isEachNotNull condition" in {
      assert(
        ColumnCondition.isEachNotNull(Seq("att1", "att2", "att3")) ==
        "(((att1 IS NOT NULL) AND (att2 IS NOT NULL)) AND (att3 IS NOT NULL))"
      )
    }

    "return the correct isAnyNotNull condition" in {
      assert(
        ColumnCondition.isAnyNotNull(Seq("att1", "att2", "att3")) ==
          "(((att1 IS NOT NULL) OR (att2 IS NOT NULL)) OR (att3 IS NOT NULL))"
      )
    }
  }

}


