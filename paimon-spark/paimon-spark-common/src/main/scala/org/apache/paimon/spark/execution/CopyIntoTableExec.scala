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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.{CopyFileFormat, FileFormatType}
import org.apache.paimon.spark.copyinto.{CopyLoadHistoryManager, CopyLoadRecord}
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataField

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.{col, input_file_name, lit, when}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class CopyIntoTableExec(
    spark: SparkSession,
    catalog: TableCatalog,
    ident: Identifier,
    sourcePath: String,
    columns: Option[Seq[String]],
    fileFormat: CopyFileFormat,
    pattern: Option[String],
    force: Boolean,
    out: Seq[Attribute])
  extends PaimonLeafV2CommandExec
  with Logging {

  override def output: Seq[Attribute] = out

  override protected def run(): Seq[InternalRow] = {
    fileFormat.validateForImport()

    val table = catalog.loadTable(ident)
    assert(table.isInstanceOf[SparkTable])
    val paimonTable = table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
    val tableSchema = paimonTable.schema()
    val writableColumns = tableSchema.fieldNames().asScala.toSeq
    val fields = tableSchema.fields().asScala.toSeq
    val targetColumns = resolveTargetColumns(writableColumns)

    validateNonNullableDefaults(writableColumns, targetColumns, fields)

    val (filesToLoad, skippedFiles) = listAndFilterFiles(paimonTable)

    if (filesToLoad.isEmpty) {
      return buildSkippedResults(skippedFiles)
    }

    val filePaths = filesToLoad.map(_.getPath.toString)
    val readerOptions = fileFormat.toSparkReaderOptions

    fileFormat.formatType match {
      case FileFormatType.PARQUET =>
        runParquetImport(
          paimonTable,
          filePaths,
          targetColumns,
          writableColumns,
          fields,
          filesToLoad,
          skippedFiles,
          readerOptions)
      case _ =>
        runTextImport(
          paimonTable,
          filePaths,
          targetColumns,
          writableColumns,
          fields,
          filesToLoad,
          skippedFiles,
          readerOptions)
    }
  }

  /**
   * Parquet import pipeline. Unlike CSV/JSON which read as strings then cast, Parquet files already
   * have typed columns, so the flow is:
   *   1. Read source Parquet with native types
   *   2. Project and cast columns to match target table schema (by column name, not position)
   *   3. Validate that no non-null values become null after casting (detect type incompatibility)
   *   4. Write to Paimon table
   *   5. Record load history for idempotent re-runs (FORCE=FALSE dedup)
   */
  private def runParquetImport(
      paimonTable: FileStoreTable,
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val rawDf = spark.read.options(readerOptions).parquet(filePaths: _*)

    val selectedDf = buildParquetDataFrame(rawDf, targetColumns, writableColumns, fields)
    validateParquetCast(rawDf, targetColumns, writableColumns, fields)

    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    selectedDf.write.format("paimon").mode("append").insertInto(tableName)

    val countDf = spark.read.options(readerOptions).parquet(filePaths: _*)
    recordHistoryAndBuildResults(paimonTable, filesToLoad, skippedFiles, countDf)
  }

  /**
   * Build the projection DataFrame for Parquet import. Maps source columns to target table columns
   * by name (case-insensitive). For each writable column:
   *   - If it's in targetColumns AND exists in source: cast source column to target type
   *   - If it's in targetColumns but missing from source: fill with NULL
   *   - If it's NOT in targetColumns (unmapped): fill with default value or NULL
   */
  private def buildParquetDataFrame(
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
   * Validate that casting Parquet source columns to target types does not silently lose data.
   * Detection strategy: if a source value is non-null but becomes null after casting, the cast
   * failed (e.g., a string "abc" cast to IntegerType → null). Aborts immediately on first failure.
   */
  private def validateParquetCast(
      rawDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): Unit = {
    val resolver = spark.sessionState.conf.resolver
    val sourceColumns = rawDf.columns.toSeq

    val castCheckCols = ArrayBuffer[(String, String)]()
    var validationDf = rawDf

    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        if (targetColumns.exists(tc => resolver(tc, colName))) {
          sourceColumns.find(s => resolver(s, colName)).foreach {
            srcColName =>
              val sparkType =
                org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
              val castColName = s"__pq_cv_$colName"
              validationDf = validationDf.withColumn(castColName, col(srcColName).cast(sparkType))
              castCheckCols += ((srcColName, castColName))
          }
        }
    }

    if (castCheckCols.nonEmpty) {
      val badCastFilter = castCheckCols
        .map { case (src, dst) => col(src).isNotNull && col(dst).isNull }
        .reduce(_ || _)
      val badRows = validationDf.filter(badCastFilter).limit(1).collect()
      if (badRows.nonEmpty) {
        val example = castCheckCols.find {
          case (src, dst) =>
            val row = badRows(0)
            val srcIdx = validationDf.schema.fieldIndex(src)
            val dstIdx = validationDf.schema.fieldIndex(dst)
            !row.isNullAt(srcIdx) && row.isNullAt(dstIdx)
        }
        throw new IllegalArgumentException(
          s"ON_ERROR = ABORT_STATEMENT: Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type.")
      }
    }
  }

  /**
   * Text-based (CSV/JSON) import pipeline. Reads all columns as strings first, then:
   *   1. Rename positional columns (CSV) or keep named columns (JSON)
   *   2. Fill unmapped columns with default values
   *   3. Cast all string columns to target types with validation
   *   4. Write to Paimon table
   *   5. Record load history
   */
  private def runTextImport(
      paimonTable: FileStoreTable,
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val stringSchema = buildStringSchema(targetColumns)

    val sourceDf = readSourceData(filePaths, stringSchema, readerOptions)
    val finalDf =
      buildFinalDataFrame(sourceDf, targetColumns, writableColumns, fields)
    val castedDf = castAndValidate(finalDf, writableColumns, fields)

    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    castedDf.write.format("paimon").mode("append").insertInto(tableName)

    val countDf = fileFormat.formatType match {
      case FileFormatType.JSON =>
        spark.read.options(readerOptions).schema(stringSchema).json(filePaths: _*)
      case _ =>
        spark.read.options(readerOptions).schema(stringSchema).csv(filePaths: _*)
    }
    recordHistoryAndBuildResults(paimonTable, filesToLoad, skippedFiles, countDf)
  }

  private def buildStringSchema(targetColumns: Seq[String]): StructType = {
    fileFormat.formatType match {
      case FileFormatType.JSON =>
        StructType(targetColumns.map(name => StructField(name, StringType, nullable = true)))
      case _ =>
        StructType(
          (0 until targetColumns.size).map(i => StructField(s"_c$i", StringType, nullable = true)))
    }
  }

  private def resolveTargetColumns(writableColumns: Seq[String]): Seq[String] = {
    columns match {
      case Some(cols) =>
        val resolver = spark.sessionState.conf.resolver
        cols.indices.foreach {
          i =>
            cols.indices.filter(_ > i).foreach {
              j =>
                if (resolver(cols(i), cols(j))) {
                  throw new IllegalArgumentException(
                    s"Duplicate columns in column list: ${cols(i)}")
                }
            }
        }
        cols.map {
          c =>
            writableColumns.find(w => resolver(w, c)).getOrElse {
              throw new IllegalArgumentException(
                s"Column '$c' does not exist in target table. Available columns: ${writableColumns.mkString(", ")}")
            }
        }
      case None => writableColumns
    }
  }

  private def validateNonNullableDefaults(
      writableColumns: Seq[String],
      targetColumns: Seq[String],
      fields: Seq[DataField]): Unit = {
    if (columns.isEmpty) return
    val unmapped = writableColumns.filterNot(targetColumns.contains)
    unmapped.foreach {
      colName =>
        val field = fields.find(_.name() == colName).get
        if (!field.`type`().isNullable && field.defaultValue() == null) {
          throw new IllegalArgumentException(
            s"Non-nullable column '$colName' is not in the column list and has no default value")
        }
    }
  }

  private def listAndFilterFiles(
      paimonTable: FileStoreTable): (Array[FileStatus], Array[FileStatus]) = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fsPath = new Path(sourcePath)
    val fs = fsPath.getFileSystem(hadoopConf)
    val allFiles = fs.listStatus(fsPath).filter(_.isFile)

    val patternFiltered = pattern match {
      case Some(p) =>
        val regex = p.r
        allFiles.filter(f => regex.findFirstIn(f.getPath.getName).isDefined)
      case None => allFiles
    }

    if (patternFiltered.isEmpty) {
      return (Array.empty, Array.empty)
    }

    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)

    if (!force) {
      val (skip, load) = patternFiltered.partition {
        f => historyManager.isLoaded(f.getPath.toString, f.getLen, f.getModificationTime)
      }
      (load, skip)
    } else {
      (patternFiltered, Array.empty[FileStatus])
    }
  }

  private def readSourceData(
      filePaths: Array[String],
      stringSchema: StructType,
      readerOptions: Map[String, String]): DataFrame = {
    var df = fileFormat.formatType match {
      case FileFormatType.JSON =>
        spark.read.options(readerOptions).schema(stringSchema).json(filePaths: _*)
      case _ =>
        spark.read.options(readerOptions).schema(stringSchema).csv(filePaths: _*)
    }

    val nullIfVals = fileFormat.nullIfValues
    if (nullIfVals.nonEmpty) {
      df.columns.foreach {
        colName =>
          df = df.withColumn(
            colName,
            when(col(colName).isin(nullIfVals: _*), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    if (fileFormat.emptyFieldAsNull) {
      df.columns.foreach {
        colName =>
          df = df.withColumn(
            colName,
            when(col(colName) === lit(""), lit(null).cast(StringType))
              .otherwise(col(colName)))
      }
    }

    df
  }

  private def buildFinalDataFrame(
      sourceDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
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
      renamedDf.select(selectExprs: _*)
    } else {
      renamedDf
    }
  }

  private def castAndValidate(
      finalDf: DataFrame,
      writableColumns: Seq[String],
      fields: Seq[DataField]): DataFrame = {
    val nonStringCastCols = ArrayBuffer[String]()
    var castedDf = finalDf
    writableColumns.zip(fields).foreach {
      case (colName, field) =>
        val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
        castedDf = castedDf.withColumn(colName, col(colName).cast(sparkType))
        if (sparkType != StringType) {
          nonStringCastCols += colName
        }
    }

    if (nonStringCastCols.nonEmpty) {
      val castSuffix = "__cv"
      val validationDf = nonStringCastCols.zipWithIndex.foldLeft(finalDf) {
        case (df, (colName, idx)) =>
          val field = fields.find(_.name() == colName).get
          val sparkType = org.apache.paimon.spark.SparkTypeUtils.fromPaimonType(field.`type`())
          df.withColumn(castSuffix + idx, col(colName).cast(sparkType))
      }
      val badCastFilter = nonStringCastCols.zipWithIndex
        .map { case (cn, idx) => col(cn).isNotNull && col(castSuffix + idx).isNull }
        .reduce(_ || _)
      val badRows = validationDf.filter(badCastFilter).limit(1).collect()
      if (badRows.nonEmpty) {
        val example = nonStringCastCols.zipWithIndex.find {
          case (cn, idx) =>
            val row = badRows(0)
            val srcIdx = validationDf.schema.fieldIndex(cn)
            val dstIdx = validationDf.schema.fieldIndex(castSuffix + idx)
            !row.isNullAt(srcIdx) && row.isNullAt(dstIdx)
        }
        throw new IllegalArgumentException(
          s"ON_ERROR = ABORT_STATEMENT: Cast failure in column '${example.map(_._1).getOrElse("unknown")}'. Source data contains values that cannot be converted to the target type.")
      }
    }

    castedDf
  }

  /**
   * Record successfully loaded files to load history (for FORCE=FALSE idempotent dedup), and build
   * the result rows showing per-file load status. Accepts a pre-built countDf that will be grouped
   * by input_file_name() to get per-file row counts — this allows both Parquet and text paths to
   * share the same logic.
   */
  private def recordHistoryAndBuildResults(
      paimonTable: FileStoreTable,
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      countDf: DataFrame): Seq[InternalRow] = {
    val paimonPath = new org.apache.paimon.fs.Path(paimonTable.location().toString)
    val historyManager = new CopyLoadHistoryManager(paimonTable.fileIO(), paimonPath)
    val snapshotId = paimonTable.snapshotManager().latestSnapshotId()
    val loadedAt = System.currentTimeMillis()

    val rowCounts = countDf
      .groupBy(input_file_name().as("file"))
      .count()
      .collect()

    val fileCountMap = rowCounts.map {
      row =>
        val fullPath = row.getString(0)
        val baseName = fullPath.substring(fullPath.lastIndexOf('/') + 1)
        baseName -> row.getLong(1)
    }.toMap

    val loadedResults = filesToLoad.map {
      fileStatus =>
        val baseName = fileStatus.getPath.getName
        val rowCount = fileCountMap.getOrElse(baseName, 0L)

        historyManager.recordLoaded(
          CopyLoadRecord(
            filePath = fileStatus.getPath.toString,
            fileSize = fileStatus.getLen,
            lastModified = fileStatus.getModificationTime,
            loadedAt = loadedAt,
            snapshotId = snapshotId,
            rowsLoaded = rowCount
          ))

        InternalRow(
          UTF8String.fromString(baseName),
          UTF8String.fromString("LOADED"),
          rowCount,
          rowCount)
    }.toSeq

    val skippedResults = buildSkippedResults(skippedFiles)
    loadedResults ++ skippedResults
  }

  private def buildSkippedResults(files: Array[FileStatus]): Seq[InternalRow] = {
    files.map {
      f =>
        InternalRow(
          UTF8String.fromString(f.getPath.getName),
          UTF8String.fromString("SKIPPED"),
          0L,
          0L)
    }.toSeq
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
            s"Failed to parse default value '$defaultVal' for column '$colName': " +
              s"${e.getMessage}. Using null instead.")
          lit(null).cast(sparkType).as(colName)
      }
    } else {
      lit(null).as(colName)
    }
  }
}
