/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.types.DataField

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Handles cast validation for COPY INTO operations. Validates that source data can be safely cast
 * to target table types without data loss.
 */
private[execution] class CopyIntoCastValidator(spark: org.apache.spark.sql.SparkSession) {

  /**
   * Build cast validation columns for Parquet import. For each writable column that exists in both
   * targetColumns and source, adds a casted temp column for comparison. Returns the augmented
   * DataFrame, the (source, cast) column pairs, and the bad-cast filter expression.
   */
  def buildParquetCastValidation(
      rawDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      excludeCols: Set[String] = Set.empty): CastValidationSetup = {
    val resolver = spark.sessionState.conf.resolver
    val sourceColumns = rawDf.columns.toSeq.filterNot(excludeCols.contains)

    val castColMapping = scala.collection.mutable.LinkedHashMap[String, String]()
    var validationDf = rawDf
    val existingCols = rawDf.columns.toSet ++ writableColumns.toSet
    var usedCols = existingCols

    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        if (targetColumns.exists(tc => resolver(tc, colName))) {
          sourceColumns.find(s => resolver(s, colName)).foreach {
            srcColName =>
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              val castColName = safeTempCol("__pq_cv_" + colName, usedCols)
              usedCols += castColName
              validationDf =
                validationDf.withColumn(castColName, nonAnsiCast(col(srcColName), sparkType))
              castColMapping(srcColName) = castColName
          }
        }
    }

    val badCastFilter = if (castColMapping.nonEmpty) {
      Some(
        castColMapping
          .map { case (src, dst) => col(src).isNotNull && col(dst).isNull }
          .reduce(_ || _))
    } else None

    CastValidationSetup(validationDf, castColMapping.toMap, badCastFilter)
  }

  /**
   * Build cast validation columns for text-based import. For each non-string writable column, adds
   * a casted temp column for comparison. Returns the augmented DataFrame, the column names
   * requiring validation, a mapping from original to temp column names, and the bad-cast filter.
   */
  def buildTextCastValidation(
      df: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField]): CastValidationSetup = {
    val existingCols = df.columns.toSet ++ writableColumns.toSet
    val castColMapping = scala.collection.mutable.LinkedHashMap[String, String]()
    var usedCols = existingCols
    var validationDf = df

    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
        if (sparkType != StringType) {
          val tempName = safeTempCol("__cv_" + colName, usedCols)
          usedCols += tempName
          castColMapping(colName) = tempName
          validationDf = validationDf.withColumn(tempName, nonAnsiCast(col(colName), sparkType))
        }
    }

    if (castColMapping.isEmpty) {
      return CastValidationSetup(df, Map.empty, None)
    }

    val badCastFilter = castColMapping
      .map { case (src, dst) => col(src).isNotNull && col(dst).isNull }
      .reduce(_ || _)

    CastValidationSetup(validationDf, castColMapping.toMap, Some(badCastFilter))
  }

  /**
   * Validate Parquet cast and abort on first failure. Detection strategy: if a source value is
   * non-null but becomes null after casting, the cast failed (e.g., a string "abc" cast to
   * IntegerType → null). Aborts immediately on first failure.
   */
  def validateParquetCast(
      rawDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): Unit = {
    val setup = buildParquetCastValidation(rawDf, targetColumns, writableColumns, fields)
    abortOnCastFailure(setup)
  }

  /** Abort on first cast failure found in the validation setup. */
  def abortOnCastFailure(setup: CastValidationSetup): Unit = {
    setup.badCastFilter.foreach {
      filter =>
        val badRows = setup.validationDf.filter(filter).limit(1).collect()
        if (badRows.nonEmpty) {
          val example = setup.castColMapping.find {
            case (src, dst) =>
              val row = badRows(0)
              val srcIdx = setup.validationDf.schema.fieldIndex(src)
              val dstIdx = setup.validationDf.schema.fieldIndex(dst)
              !row.isNullAt(srcIdx) && row.isNullAt(dstIdx)
          }
          throw new IllegalArgumentException(
            s"ON_ERROR = ABORT_STATEMENT: Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type.")
        }
    }
  }

  /**
   * Cast all writable columns to their target Paimon types and validate. Used for text-based ABORT
   * mode.
   */
  def castAndValidate(
      finalDf: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val castedDf = castColumns(finalDf, writableColumns, fields)

    val setup = buildTextCastValidation(finalDf, writableColumns, fields)
    abortOnCastFailure(setup)

    castedDf
  }

  /** Cast all writable columns to their target Paimon types. */
  def castColumns(
      df: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    writableColumns.zip(fields).foldLeft(df) {
      case (d, (colName, field)) =>
        val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
        d.withColumn(colName, col(colName).cast(sparkType))
    }
  }

  private def safeTempCol(baseName: String, existingColumns: Set[String]): String =
    CopyIntoHelper.safeTempCol(spark, baseName, existingColumns)

  /**
   * Cast a column with ANSI disabled so a failed cast yields NULL instead of throwing. This is the
   * basis for bad-row detection: a source value that is non-null but becomes null after casting
   * could not be converted to the target type. Under Spark's default ANSI mode a plain `.cast`
   * would raise `CAST_INVALID_INPUT` before any filtering could run.
   */
  private def nonAnsiCast(column: Column, dataType: DataType): Column = {
    val expr = SparkShimLoader.shim.classicApi.expression(spark, column)
    SparkShimLoader.shim.classicApi.column(Compatibility.cast(expr, dataType, ansiEnabled = false))
  }
}

/** Unified cast validation setup for both Parquet and text (CSV/JSON) paths. */
private[execution] case class CastValidationSetup(
    validationDf: DataFrame,
    castColMapping: Map[String, String],
    badCastFilter: Option[org.apache.spark.sql.Column])
