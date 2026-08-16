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

import org.apache.paimon.spark.catalyst.plans.logical.{CopyFileFormat, FileFormatType}
import org.apache.paimon.types.DataField

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Handles DataFrame construction and transformation for COPY INTO operations. Responsible for
 * building DataFrames from source files, applying transformations, and preparing data for writing
 * to Paimon tables.
 */
private[execution] class CopyIntoDataFrameBuilder(
    spark: SparkSession,
    fileFormat: CopyFileFormat,
    columns: Option[Seq[String]])
  extends Logging {

  /**
   * Build the projection DataFrame for Parquet import. Maps source columns to target table columns
   * by name (case-insensitive). For each writable column:
   *   - If it's in targetColumns AND exists in source: cast source column to target type
   *   - If it's in targetColumns but missing from source: fill with NULL
   *   - If it's NOT in targetColumns (unmapped): fill with default value or NULL
   */
  def buildParquetDataFrame(
      rawDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val resolver = spark.sessionState.conf.resolver
    val sourceColumns = rawDf.columns.toSeq

    val selectExprs: Seq[Column] = writableColumns.map {
      colName =>
        if (targetColumns.exists(tc => resolver(tc, colName))) {
          val srcCol = sourceColumns.find(s => resolver(s, colName))
          srcCol match {
            case Some(s) =>
              val field = fields.find(_.name() == colName).get
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              col(s).cast(sparkType).as(colName)
            case None =>
              val field = fields.find(_.name() == colName).get
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              lit(null).cast(sparkType).as(colName)
          }
        } else {
          resolveDefaultColumn(fields.find(_.name() == colName).get, colName)
        }
    }
    rawDf.select(selectExprs: _*)
  }

  /**
   * Build the final DataFrame for text-based import. Handles column renaming (CSV positional →
   * named), default value filling for unmapped columns, and preserves extra columns like file name.
   */
  def buildFinalDataFrame(
      sourceDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      extraCols: Seq[String] = Seq.empty): DataFrame = {
    val renamedDf = fileFormat.formatType match {
      case FileFormatType.JSON =>
        sourceDf
      case _ =>
        targetColumns.zipWithIndex.foldLeft(sourceDf) {
          case (df, (targetCol, idx)) => df.withColumnRenamed(s"_c$idx", targetCol)
        }
    }

    if (columns.isDefined) {
      val selectExprs: Seq[Column] = writableColumns.map {
        colName =>
          if (targetColumns.contains(colName)) {
            col(colName)
          } else {
            resolveDefaultColumn(fields.find(_.name() == colName).get, colName)
          }
      }
      renamedDf.select((selectExprs ++ extraCols.map(col)): _*)
    } else {
      renamedDf
    }
  }

  /** Build string schema for text-based formats (CSV/JSON). */
  def buildStringSchema(targetColumns: Seq[String]): StructType = {
    fileFormat.formatType match {
      case FileFormatType.JSON =>
        StructType(targetColumns.map(name => StructField(name, StringType, nullable = true)))
      case _ =>
        StructType(
          (0 until targetColumns.size).map(i => StructField(s"_c$i", StringType, nullable = true)))
    }
  }

  /** Read source data for text-based formats with NULL transforms applied. */
  def readSourceData(
      filePaths: Array[String],
      stringSchema: StructType,
      readerOptions: Map[String, String]): DataFrame = {
    val df = fileFormat.formatType match {
      case FileFormatType.JSON =>
        spark.read.options(readerOptions).schema(stringSchema).json(filePaths: _*)
      case _ =>
        spark.read.options(readerOptions).schema(stringSchema).csv(filePaths: _*)
    }

    applyNullTransforms(df, df.columns)
  }

  /** Apply NULL_IF and EMPTY_FIELD_AS_NULL transforms to the specified columns. */
  def applyNullTransforms(df: DataFrame, columns: Seq[String]): DataFrame = {
    var result = df

    val nullIfVals = fileFormat.nullIfValues
    if (nullIfVals.nonEmpty) {
      columns.foreach {
        colName =>
          result = result.withColumn(
            colName,
            when(col(colName).isin(nullIfVals: _*), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    if (fileFormat.emptyFieldAsNull) {
      columns.foreach {
        colName =>
          result = result.withColumn(
            colName,
            when(col(colName) === lit(""), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    result
  }

  /** Resolve the default value expression for a column not populated from source data. */
  private def resolveDefaultColumn(field: DataField, colName: String): Column = {
    val defaultVal = field.defaultValue()
    if (defaultVal != null) {
      val sparkType =
        org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
      try {
        val parsed = spark.sessionState.sqlParser.parseExpression(defaultVal)
        SparkShimLoader.shim.classicApi.column(parsed).cast(sparkType).as(colName)
      } catch {
        case e: Exception =>
          logWarning(
            s"Failed to parse default value '$defaultVal' for column '$colName'; using NULL instead.",
            e)
          lit(null).cast(sparkType).as(colName)
      }
    } else {
      lit(null).as(colName)
    }
  }
}
