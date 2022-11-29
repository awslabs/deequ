package com.amazon.deequ.analyzers

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.FullColumn
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompletenessTest extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {
  def completenessSampleData(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    // Example from https://github.com/awslabs/deequ/issues/178
    Seq(
      ("India", "Xavier House, 2nd Floor", "St. Peter Colony, Perry Road", "Bandra (West)"),
      ("India", "503 Godavari", "Sir Pochkhanwala Road", "Worli"),
      ("India", "4/4 Seema Society", "N Dutta Road, Four Bungalows", "Andheri"),
      ("India", "1001D Abhishek Apartments", "Juhu Versova Road", "Andheri"),
      ("India", "95, Hill Road", null, null),
      ("India", "90 Cuffe Parade", "Taj President Hotel", "Cuffe Parade"),
      ("India", "4, Seven PM", "Sir Pochkhanwala Rd", "Worli"),
      ("India", "1453 Sahar Road", null, null)
    )
      .toDF("Country", "Address Line 1", "Address Line 2", "Address Line 3")
  }

  "Completeness" should {
    "return row-level results for columns" in withSparkSession { session =>

      val data = completenessSampleData(session)

      val completenessCountry = Completeness("Address Line 3")
      val state = completenessCountry.computeStateFrom(data)
      val metric: DoubleMetric with FullColumn = completenessCountry.computeMetricFrom(state)

      data.withColumn("new", metric.fullColumn.get).show()
    }
  }
}
