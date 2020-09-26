package com.amazon.deequ.examples

import java.math.BigDecimal
import java.sql.Timestamp


import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.{BigDecimalMean, BigDecimalSum, DateTimeDistribution, DistributionInterval, MaximumDateTime, Mean, MinimumDateTime, Sum}
import com.amazon.deequ.examples.ExampleUtils.{ordersAsDataframe, withSpark}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame

private[examples] object AnalyzerExample extends App {
  withSpark { session =>

    val data = ordersAsDataframe(session,
      Order(1, new BigDecimal(213.2132), Timestamp.valueOf("2020-02-15 07:15:00")),
      Order(2, new BigDecimal(43.21324432876), Timestamp.valueOf("2020-02-15 07:45:00.999")),
      Order(3, new BigDecimal(56.8881238823888), Timestamp.valueOf("2020-02-15 08:15:49.786")),
      Order(4, new BigDecimal(101.2324434978788), Timestamp.valueOf("2020-02-15 12:15:00")),
      Order(5, new BigDecimal(723.234324234324324324), Timestamp.valueOf("2020-02-15 15:14:23.678"))
    )

    val analysisResult: AnalyzerContext = { AnalysisRunner
      .onData(data)
      .addAnalyzer(DateTimeDistribution("orderDate", DistributionInterval.HOURLY))
      .addAnalyzer(MinimumDateTime("orderDate"))
      .addAnalyzer(MaximumDateTime("orderDate"))
      .addAnalyzer(Sum("amount"))
      .addAnalyzer(BigDecimalSum("amount"))
      .addAnalyzer(Mean("amount"))
      .addAnalyzer(BigDecimalMean("amount"))
      .run()
    }

    successMetricsAsDataFrame(session, analysisResult).show(false)

    analysisResult.metricMap.foreach( x =>
      println(s"column '${x._2.instance}' has ${x._2.name} : ${x._2.value.get}")
    )

  }
}
