/**
 * Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.deequ.comparison

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.hash
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Compare two DataFrames 1 to 1 with specific columns inputted by the customer.
 *
 * This is an experimental utility.
 *
 * For example, consider the two dataframes below:
 *
 * DataFrame A:
 *
 * |--ID--|---City---|--State--|
 * |  1   | New York |    NY   |
 * |  2   | Chicago  |    IL   |
 * |  3   | Boston   |    MA   |
 *
 * DataFrame B:
 *
 * |--CityID--|---City---|-----State-----|
 * |     1    | New York |    New York   |
 * |     2    | Chicago  |    Illinois   |
 * |     3    | Boston   | Massachusetts |
 *
 * Note that dataframe B is almost equal to dataframe B, but for two things:
 *  1) The ID column in B is called CityID
 *  2) The State column in B is the full name, whereas A uses the abbreviation.
 *
 * To compare A with B, for just the City column, we can use the function like the following.
 *
 * DataSynchronization.columnMatch(
 *   ds1 = dfA,
 *   ds2 = dfB,
 *   colKeyMap = Map("ID" -> "CityID"), // Mapping for the key columns
 *   compCols = Map("City" -> "City"), // Mapping for the columns that should be compared
 *   assertion = _ > 0.8
 * )
 *
 * This will evaluate to true since the City column matches in A and B for the corresponding ID.
 *
 * To compare A with B, for all columns, we can use the function like the following.
 *
 * DataSynchronization.columnMatch(
 *   ds1 = dfA,
 *   ds2 = dfB,
 *   colKeyMap = Map("ID" -> "CityID"), // Mapping for the key columns
 *   assertion = _ > 0.8
 * )
 *
 * This will evaluate to false. The city column will match, but the state column will not.
 */

object DataSynchronization extends ComparisonBase {
  /**
   * This will evaluate to false. The city column will match, but the state column will not.
   *
   * @param ds1                The first data set which the customer will select for comparison.
   * @param ds2                The second data set which the customer will select for comparison.
   * @param colKeyMap          A map of columns to columns used for joining the two datasets.
   *                           The keys in the map are composite key forming columns from the first dataset.
   *                           The values for each key is the equivalent column from the second dataset.
   * @param assertion          A function which accepts the match ratio and returns a Boolean.
   * @return ComparisonResult  An appropriate subtype of ComparisonResult is returned.
   *                           Once all preconditions are met, we calculate the ratio of the rows
   *                           that match and we run the assertion on that outcome.
   *                           The response is then converted to ComparisonResult.
   */
  def columnMatch(ds1: DataFrame,
                  ds2: DataFrame,
                  colKeyMap: Map[String, String],
                  assertion: Double => Boolean): ComparisonResult = {
    val columnErrors = areKeyColumnsValid(ds1, ds2, colKeyMap)
    if (columnErrors.isEmpty) {
      // Get all the non-key columns from DS1 and verify that they are present in DS2
      val colsDS1 = ds1.columns.filterNot(x => colKeyMap.keys.toSeq.contains(x)).sorted
      val nonKeyColsMatch = colsDS1.forall(columnExists(ds2, _))

      if (!nonKeyColsMatch) {
        DatasetMatchFailed("Non key columns in the given data frames do not match.")
      } else {
        val mergedMaps = colKeyMap ++ colsDS1.map(x => x -> x).toMap
        finalAssertion(ds1, ds2, mergedMaps, assertion)
      }
    } else {
      DatasetMatchFailed(columnErrors.get)
    }
  }

  /**
   * This will evaluate to false. The city column will match, but the state column will not.
   *
   * @param ds1                The first data set which the customer will select for comparison.
   * @param ds2                The second data set which the customer will select for comparison.
   * @param colKeyMap          A map of columns to columns used for joining the two datasets.
   *                           The keys in the map are composite key forming columns from the first dataset.
   *                           The values for each key is the equivalent column from the second dataset.
   * @param compCols           A map of columns to columns which we will check for equality, post joining.
   * @param assertion          A function which accepts the match ratio and returns a Boolean.
   * @return ComparisonResult  An appropriate subtype of ComparisonResult is returned.
   *                           Once all preconditions are met, we calculate the ratio of the rows
   *                           that match and we run the assertion on that outcome.
   *                           The response is then converted to ComparisonResult.
   */
  def columnMatch(ds1: DataFrame,
                  ds2: DataFrame,
                  colKeyMap: Map[String, String],
                  compCols: Map[String, String],
                  assertion: Double => Boolean): ComparisonResult = {
    val keyColumnErrors = areKeyColumnsValid(ds1, ds2, colKeyMap)
    if (keyColumnErrors.isEmpty) {
      val nonKeyColumns1NotInDataset = compCols.keys.filterNot(columnExists(ds1, _))
      val nonKeyColumns2NotInDataset = compCols.values.filterNot(columnExists(ds2, _))

      if (nonKeyColumns1NotInDataset.nonEmpty) {
        DatasetMatchFailed(s"The following columns were not found in the first dataset: " +
          s"${nonKeyColumns1NotInDataset.mkString(", ")}")
      } else if (nonKeyColumns2NotInDataset.nonEmpty) {
        DatasetMatchFailed(s"The following columns were not found in the second dataset: " +
          s"${nonKeyColumns2NotInDataset.mkString(", ")}")
      } else {
        val mergedMaps = colKeyMap ++ compCols
        finalAssertion(ds1, ds2, mergedMaps, assertion)
      }
    } else {
      DatasetMatchFailed(keyColumnErrors.get)
    }
  }

  def columnMatchRowLevel(ds1: DataFrame,
                          ds2: DataFrame,
                          colKeyMap: Map[String, String],
                          optionalCompCols: Option[Map[String, String]] = None,
                          optionalOutcomeColumnName: Option[String] = None):
  Either[DatasetMatchFailed, DataFrame] = {
    val columnErrors = areKeyColumnsValid(ds1, ds2, colKeyMap)
    if (columnErrors.isEmpty) {
      val compColsEither: Either[DatasetMatchFailed, Map[String, String]] = if (optionalCompCols.isDefined) {
        optionalCompCols.get match {
          case compCols if compCols.isEmpty => Left(DatasetMatchFailed("Empty column comparison map provided."))
          case compCols =>
            val ds1CompColsNotInDataset = compCols.keys.filterNot(columnExists(ds1, _))
            val ds2CompColsNotInDataset = compCols.values.filterNot(columnExists(ds2, _))
            if (ds1CompColsNotInDataset.nonEmpty) {
              Left(
                DatasetMatchFailed(s"The following columns were not found in the first dataset: " +
                  s"${ds1CompColsNotInDataset.mkString(", ")}")
              )
            } else if (ds2CompColsNotInDataset.nonEmpty) {
              Left(
                DatasetMatchFailed(s"The following columns were not found in the second dataset: " +
                  s"${ds2CompColsNotInDataset.mkString(", ")}")
              )
            } else {
              Right(compCols)
            }
        }
      } else {
        // Get all the non-key columns from DS1 and verify that they are present in DS2
        val ds1NonKeyCols = ds1.columns.filterNot(x => colKeyMap.keys.toSeq.contains(x)).sorted
        val nonKeyColsMatch = ds1NonKeyCols.forall(columnExists(ds2, _))

        if (!nonKeyColsMatch) {
          Left(DatasetMatchFailed("Non key columns in the given data frames do not match."))
        } else {
          Right(ds1NonKeyCols.map { c => c -> c}.toMap)
        }
      }

      compColsEither.flatMap { compCols =>
        val outcomeColumn = optionalOutcomeColumnName.getOrElse(defaultOutcomeColumnName)
        Try { columnMatchRowLevelInner(ds1, ds2, colKeyMap, compCols, outcomeColumn) } match {
          case Success(df) => Right(df)
          case Failure(ex) =>
            ex.printStackTrace()
            Left(DatasetMatchFailed(s"Comparison failed due to ${ex.getCause.getClass}"))
        }
      }
    } else {
      Left(DatasetMatchFailed(columnErrors.get))
    }
  }

  private def areKeyColumnsValid(ds1: DataFrame,
                                 ds2: DataFrame,
                                 colKeyMap: Map[String, String]): Option[String] = {
    val ds1Cols = colKeyMap.keys.toSeq
    val ds2Cols = colKeyMap.values.toSeq

    val ds1ColsNotInDataset = ds1Cols.filterNot(columnExists(ds1, _))
    val ds2ColsNotInDataset = ds2Cols.filterNot(columnExists(ds2, _))

    if (ds1ColsNotInDataset.nonEmpty) {
      Some(s"The following key columns were not found in the first dataset: ${ds1ColsNotInDataset.mkString(", ")}")
    } else if (ds2ColsNotInDataset.nonEmpty) {
      Some(s"The following key columns were not found in the second dataset: ${ds2ColsNotInDataset.mkString(", ")}")
    } else {
      // We verify that the key columns provided form a valid primary/composite key.
      // To achieve this, we group the dataframes and compare their count with the original count.
      // If the key columns provided are valid, then the two counts should match.
      val ds1Unique = ds1.groupBy(ds1Cols.map(col): _*).count()
      val ds2Unique = ds2.groupBy(ds2Cols.map(col): _*).count()

      val ds1Count = ds1.count()
      val ds2Count = ds2.count()
      val ds1UniqueCount = ds1Unique.count()
      val ds2UniqueCount = ds2Unique.count()

      if (ds1UniqueCount == ds1Count && ds2UniqueCount == ds2Count) {
        None
      } else {
        val combo1 = ds1Cols.mkString(", ")
        val combo2 = ds2Cols.mkString(", ")
        Some(s"The selected columns are not comparable due to duplicates present in the dataset." +
          s"Comparison keys must be unique, but " +
          s"in Dataframe 1, there are $ds1UniqueCount unique records and $ds1Count rows," +
          s" and " +
          s"in Dataframe 2, there are $ds2UniqueCount unique records and $ds2Count rows, " +
          s"based on the combination of keys {$combo1} in Dataframe 1 and {$combo2} in Dataframe 2")
      }
    }
  }

  private def finalAssertion(ds1: DataFrame,
                             ds2: DataFrame,
                             mergedMaps: Map[String, String],
                             assertion: Double => Boolean): ComparisonResult = {

    val ds1Count = ds1.count()
    val ds2Count = ds2.count()

    if (ds1Count != ds2Count) {
      DatasetMatchFailed(s"The row counts of the two data frames do not match.")
    } else {
      val joinExpression: Column = mergedMaps
        .map { case (col1, col2) => ds1(col1) === ds2(col2)}
        .reduce((e1, e2) => e1 && e2)

      val joined = ds1.join(ds2, joinExpression, "inner")
      val passedCount = joined.count()
      val totalCount = ds1Count
      val ratio = passedCount.toDouble / totalCount.toDouble

      if (assertion(ratio)) {
        DatasetMatchSucceeded(passedCount, totalCount)
      } else {
        DatasetMatchFailed(s"Data Synchronization Comparison Metric Value: $ratio does not meet the constraint" +
          s"requirement.", Some(passedCount), Some(totalCount))
      }
    }
  }

  private def columnMatchRowLevelInner(ds1: DataFrame,
                                       ds2: DataFrame,
                                       colKeyMap: Map[String, String],
                                       compCols: Map[String, String],
                                       outcomeColumnName: String): DataFrame = {
    // We sort in case .keys / .values do not return the elements in the same order
    val ds1KeyCols = colKeyMap.keys.toSeq.sorted
    val ds2KeyCols = colKeyMap.values.toSeq.sorted

    val ds1HashColName = java.util.UUID.randomUUID().toString
    val ds2HashColName = java.util.UUID.randomUUID().toString

    // The hashing allows us to check the equality without having to check cell by cell
    val ds1HashCol = hash(compCols.keys.toSeq.sorted.map(col): _*)
    val ds2HashCol = hash(compCols.values.toSeq.sorted.map(col): _*)

    // We need to update the names of the columns in ds2 so that they do not clash with ds1 when we join
    val ds2KeyColsUpdatedNamesMap = ds2KeyCols.zipWithIndex.map {
      case (col, i) => col -> s"${referenceColumnNamePrefix}_$i"
    }.toMap

    val ds1WithHashCol = ds1.withColumn(ds1HashColName, ds1HashCol)

    val ds2ReductionSeed = ds2
      .withColumn(ds2HashColName, ds2HashCol)
      .select((ds2KeyColsUpdatedNamesMap.keys.toSeq :+ ds2HashColName).map(col): _*)

    val ds2Reduced = ds2KeyColsUpdatedNamesMap.foldLeft(ds2ReductionSeed) {
      case (accumulatedDF, (origCol, updatedCol)) =>
        accumulatedDF.withColumn(updatedCol, col(origCol)).drop(origCol)
    }

    val joinExpression: Column = ds1KeyCols
      .map { ds1KeyCol => ds1KeyCol -> ds2KeyColsUpdatedNamesMap(colKeyMap(ds1KeyCol)) }
      .map { case (ds1Col, ds2ReducedCol) => ds1WithHashCol(ds1Col) === ds2Reduced(ds2ReducedCol) }
      .reduce((e1, e2) => e1 && e2) && ds1WithHashCol(ds1HashColName) === ds2Reduced(ds2HashColName)

    // After joining, we will have:
    //  - All the columns from ds1
    //  - A column containing the hash of the non key columns from ds1
    //  - All the key columns from ds2, with updated column names
    //  - A column containing the hash of the non key columns from ds2
    val joined = ds1WithHashCol.join(ds2Reduced, joinExpression, "left")

    // In order for rows to match,
    // - The key columns from ds2 must be in the joined dataframe, i.e. not null
    // - The hash column of the non key cols from ds2 must be in the joined dataframe, i.e. not null
    val filterExpression = ds2KeyColsUpdatedNamesMap.values
      .map { ds2ReducedCol => col(ds2ReducedCol).isNotNull }
      .reduce((e1, e2) => e1 && e2)

    joined
      .withColumn(outcomeColumnName, when(filterExpression, lit(true)).otherwise(lit(false)))
      .drop(ds1HashColName)
      .drop(ds2HashColName)
      .drop(ds2KeyColsUpdatedNamesMap.values.toSeq: _*)
  }

  private def columnExists(df: DataFrame, col: String) = Try { df(col) }.isSuccess
}
