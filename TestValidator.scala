import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark.implicits._
import org.apache.spark.sql.functions.{ count, col, lit, expr, length, not, unix_timestamp, regexp_extract, when, concat, concat_ws}

// import org.apache.spark.sql.types.{Any}
    

// , ("victor", -1)
// var df = List(("noah", 1), ("ryan", 2),("harris", 3), ("incorrect", -1), ("wrong", "Unknown")).toDF("name", "id")
var df_bad: DataFrame = Seq(("Noah", "1"), ("harris", "Unknown"), ("ryan", "-1"), ("H", "5"), ("G", "4"), ("D", "wrong"), ("J", "101")).toDF("name", "id")

// val added_row = [["wrong", "Unknown"]]
// val added_row = Seq(("wrong", "Unknown")).toDF("name", "id")

// val added_row_df = spark.createDataFrame(added_row)
// df = df.union(added_row_df)

val schema = RowLevelSchema().isNonNegative("id", isNullable = false).withIntColumn("id", isNullable = false)
// .withIntColumn("id", isNullable = false)
// val check = RowLevelChecks().isNonNegative("id")

// val resultSchemaOnly = RowLevelSchemaValidator.validate(df, schema)
// val resultChecksOnly = RowLevelSchemaValidator.validate(df,  check)


val resultBoth = RowLevelSchemaValidator.validate(df_bad, schema)
// val resultBoth = RowLevelSchemaValidator.validate(df_bad, schema)

// val valid = resultBoth.validRows.select("*")
// val invalid = resultBoth.invalidRows.select("*")
// val numV = resultBoth.numValidRows
// val numI = resultBoth.numInvalidRows
// val stats = resultBoth.stats

// var nameList = Seq[String]()
// nameList = nameList :+ "idisNonNegativeCheck"
// nameList = nameList :+ "idIntegerSchema"

// var num_false = val tot
// nameList.foreach{ name =>
//     // dataWithMatches = dataWithMatches.sum(failedColumn, when(col(name) === false, concat_ws("|" , lit(name), col(failedColumn))).otherwise((col(failedColumn))))
//     // var test = invalid.select(col(name)==false, col(name).count())
//     var df = invalid.filter(!invalid(name))
//     // var df = invalid.select(when(col(name)== false))
//     num_false += (name -> invalid.filter(!invalid(name)).count()/3)

    
//         // .drop(col(name))
// }

// num_false.toSeq.toDF("name", "percent_failed")
//     storageLevelForIntermediateResults: StorageLevel = StorageLevel.MEMORY_AND_DISK



