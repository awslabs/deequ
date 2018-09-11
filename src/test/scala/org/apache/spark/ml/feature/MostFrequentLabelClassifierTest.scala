package org.apache.spark.ml.feature

import com.amazon.deequ.SparkContextSpec
import com.amazon.deequ.imputation.MostFrequentLabelClassifier
import org.apache.spark.ml.linalg.Vectors
import org.scalatest._

class MostFrequentLabelClassifierTest extends WordSpec with Matchers with SparkContextSpec {
  "A MostFrequentLabelClassifier" should {
    "predict the most frequent value of a column" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val df = sparkSession.createDataFrame(Seq(
        (1.0, Vectors.dense(1.0)),
        (1.0, Vectors.dense(1.0)),
        (1.0, Vectors.dense(1.0)),
        (0.0, Vectors.dense(0.0))
      )).toDF("label", "features")

      val model = new MostFrequentLabelClassifier()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .fit(df)

      model.transform(df)
        .select("prediction").as[Double]
        .collect should be(Array(1.0, 1.0, 1.0, 1.0))
    }
  }
}

