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

import ExampleUtils.{itemsAsDataframe, withSpark}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.repository.MetricsRepositoryDiff
import com.google.common.io.Files
private[examples] object MetricsDiffExample  extends App  {

  withSpark { session =>

    val data = itemsAsDataframe(session,
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    val moreData = itemsAsDataframe(session,
      Item(4, "Thingy D", null, "low", 10),
      Item(5, "Thingy E", null, "high", 12))

    val evenMoreData = itemsAsDataframe(session,
      Item(4, "Thingy F", null, "low", 10),
      Item(5, "Thingy G", null, "high", 12))

    val analyzers = Seq(Maximum("numViews"), Sum("numViews"), CountDistinct("numViews"))

    val repository: MetricsRepositoryDiff = new MetricsRepositoryDiff(
      session, Files.createTempDir().getAbsolutePath, true)

    repository.aggregateMultipleMetricsRepositories(Seq(data, moreData, evenMoreData), analyzers)
  }
}
