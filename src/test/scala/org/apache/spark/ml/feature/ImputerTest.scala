package com.amazon.deequ

import com.amazon.deequ.imputation.{Imputer, ImputerModel, MostFrequentLabelClassifier}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

class ImputerTest extends WordSpec with Matchers with SparkContextSpec {

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.INFO)

  /** UDF that discards values */
  val nullifyEntriesUdf: UserDefinedFunction = udf { (entry: String, nullifyRatio: Double) =>
    if (breeze.stats.distributions.Rand.uniform.draw < nullifyRatio) null else entry
  }

  def setNullableStateOfColumn(df: DataFrame, cn: String, nullable: Boolean): DataFrame = {

    // get schema
    val schema = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) if c.equals(cn) => StructField(c, t, nullable = nullable, m)
      case y: StructField => y
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def getDummyDf(sparkSession: SparkSession): DataFrame =
    sparkSession.createDataFrame(Seq(
      ("1", "a", "f"),
      ("2", "b", "d")
    )).toDF("asin", "att1", "att2")

  def getDummyDfMissingSmall(sparkSession: SparkSession): DataFrame =
    sparkSession.createDataFrame(Seq(
      ("1", "a", "d"),
      ("2", "b", null),
      ("3", null, "f"),
      ("4", "a", "f")
    )).toDF("asin", "att1", "att2")

  def getDummyDfMissingLarge(sparkSession: SparkSession): DataFrame =
    sparkSession.createDataFrame(Seq(
      ("1", "a", "f"),
      ("2", "b", "d"),
      ("3", null, "f"),
      ("4", "a", null),
      ("5", "a", "f"),
      ("6", "b", "d"),
      ("7", null, "d"),
      ("8", "b", null),
      ("9", "a", "f"),
      ("10", "a", null),
      ("11", null, "f"),
      ("12", "a", "f"),
      ("13", null, "d")
    )).toDF("asin", "att1", "att2")

  "A ImputerModel instance" should {

    "fail when provided with wrong number of input and output columns" in
      withSparkSession { sparkSession =>

        val df = sparkSession.createDataFrame(Seq(
          ("1", "a", "f"),
          ("2", "b", "d")
        )).toDF("asin", "att1", "att2")

        val model = new Imputer()
          .setInputCols("att1", "att2")
          .setOutputCols("att1_clean")

        intercept[IllegalArgumentException] {
          model.fit(df)
        }

      }

    "fail when provided with wrong number of input and feature columns" in
      withSparkSession { sparkSession =>

        val df = getDummyDf(sparkSession)

        val model = new Imputer()
          .setInputCols("att1", "att2")
          .setOutputCols("att1_clean", "att2_clean")
          .setFeaturesCols(Array("att2"))

        intercept[IllegalArgumentException] {
          model.fit(df)
        }

      }

    "fail when provided with input col that is missing" in withSparkSession { sparkSession =>

      val df = getDummyDf(sparkSession)

      val model = new Imputer()
        .setInputCols("att3")
        .setOutputCols("att3_clean")

      intercept[IllegalArgumentException] {
        model.fit(df)
      }

    }

    "fail when provided with output col that is amongst inputCols" in
      withSparkSession { sparkSession =>

        val df = getDummyDf(sparkSession)

        val model = new Imputer()
          .setInputCols("att2")
          .setOutputCols("att1")

        intercept[IllegalArgumentException] {
          model.fit(df)
        }

      }


    "fail when provided with feature col that is input cols" in withSparkSession { sparkSession =>

      val df = getDummyDf(sparkSession)

      val model = new Imputer()
        .setInputCols("att1")
        .setOutputCols("att1_clean")
        .setFeaturesCols(Array("att1", "att2"))

      intercept[IllegalArgumentException] {
        model.fit(df)
      }

    }

    "default to all nullable inputCols when no inputCols are provided" in
      withSparkSession { sparkSession =>

        val df = setNullableStateOfColumn(getDummyDf(sparkSession), "asin", nullable = false)

        val model = new Imputer().fit(df)

        val cols = Array("att1", "att2")
        model.getInputCols should be(cols)
      }

    "default to inputCols + suffix when no outputCols are provided" in
      withSparkSession { sparkSession =>

        val df = setNullableStateOfColumn(getDummyDf(sparkSession), "asin", nullable = false)

        val suffix = "_suffix"
        val cols = Array("att1", "att2")

        val model = new Imputer()
          .setInputCols("att1", "att2")
          .setImputedSuffix(suffix)
          .fit(df)

        model.getOutputCols should be(cols.map(_ + suffix))
      }


    "impute the most likely value of a column with a supervised model" in
      withSparkSession { sparkSession =>

        import sparkSession.implicits._

        val df = getDummyDfMissingLarge(sparkSession)

        val estimator = new Imputer()
          .setInputCols("att1", "att2")
          .setOutputCols("att1_clean", "att2_clean")
          .setNumHashBuckets(10)
          .setDataRangeSize(10)

        val model = estimator.fit(df)

        val imputed = model.transform(df)

        imputed
          .select("att1_clean")
          .as[String]
          .take(7) should be(Array("a", "b", "a", "a", "a", "b", "b"))

        imputed.select("att2_clean")
          .as[String]
          .take(8) should be(Array("f", "d", "f", "f", "f", "d", "d", "d"))
      }

    "work in default mode" in withSparkSession { sparkSession =>

      import sparkSession.implicits._

      val df = getDummyDfMissingLarge(sparkSession)

      val model = new Imputer().fit(df)

      val imputed = model.transform(df)

      imputed
        .select("att1_imputed")
        .as[String]
        .take(7) should be(Array("a", "b", "a", "a", "a", "b", "b"))

    }

    "impute the most frequent value of a column when used with MostFrequentLabelClassifier " +
      "[Mode imputation]" in withSparkSession { sparkSession =>

        import sparkSession.implicits._

        val df = getDummyDfMissingSmall(sparkSession)

        val estimator = new Imputer()
          .setInputCols("att1")
          .setOutputCols("att1_clean")
          .setDataRangeSize(10)
          .setModel(new MostFrequentLabelClassifier())

        val model = estimator.fit(df)

        model.transform(df)
          .select("att1_clean")
          .as[String]
          .collect should be(Array("a", "b", "a", "a"))
      }

    "produce the same imputations when serialized and deserialized" in
      withSparkSession { sparkSession =>

        import sparkSession.implicits._

        val df = getDummyDfMissingSmall(sparkSession)

        val estimator = new Imputer()
          .setInputCols("att1")
          .setOutputCols("att1_clean")
          .setDataRangeSize(10)
          .setModel(new NaiveBayes())

        val model = estimator.fit(df)

        val modelWriter = ImputerModel.write(model)
        val modelPath = "/tmp/serialized_test_model"
        modelWriter.overwrite().save(modelPath)

        val modelReader = ImputerModel.read
        val deserializedModel = modelReader.load(modelPath)
        deserializedModel.transform(df)
          .select("att1_clean")
          .as[String]
          .collect should be(model.transform(df)
          .select("att1_clean")
          .as[String]
          .collect)

        val deserializedModel2 = ImputerModel.load(modelPath)
        deserializedModel2.transform(df)
          .select("att1_clean")
          .as[String]
          .collect should be(model.transform(df)
          .select("att1_clean")
          .as[String]
          .collect)
      }

  }

  "return the correct data range for an attribute" in
    withSparkSession { sparkSession =>

      val df = getDummyDfMissingLarge(sparkSession)

      val estimator = new Imputer()
        .setInputCols("att1")
        .setOutputCols("att1_clean")
        .setDataRangeSize(10)
        .setModel(new MostFrequentLabelClassifier())

      val model = estimator.fit(df)

      model.dataRange("att1") should be(Array(("a", 6L), ("b", 3)))

    }

  "throw an error if wrong data range is requested" in
    withSparkSession { sparkSession =>

      val df = getDummyDfMissingLarge(sparkSession)

      val estimator = new Imputer()
        .setInputCols("att1")
        .setOutputCols("att1_clean")
        .setDataRangeSize(10)
        .setModel(new MostFrequentLabelClassifier())

      val model = estimator.fit(df)

      intercept[IllegalArgumentException] {
        model.dataRange("foo")
      }

    }

  "return the correct model for an attribute" in withSparkSession { sparkSession =>

    val df = getDummyDfMissingLarge(sparkSession)

    val estimator = new Imputer()
      .setInputCols("att1")
      .setModel(new NaiveBayes())

    val model = estimator.fit(df)

    val imputationModel = model.imputationModel("att1").asInstanceOf[PipelineModel]
    imputationModel.stages(2).uid.contains("nb") should be(true)
  }

  "throw an error if wrong imputation model is requested" in
    withSparkSession { sparkSession =>

      val df = getDummyDfMissingLarge(sparkSession)

      val estimator = new Imputer()
        .setInputCols("att1")
        .setOutputCols("att1_clean")
        .setModel(new MostFrequentLabelClassifier())

      val model = estimator.fit(df)

      intercept[IllegalArgumentException] {
        model.imputationModel("foo")
      }

    }
}
