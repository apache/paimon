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

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

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
  extends PaimonLeafV2CommandExec {

  // Initialize helper classes
  private val castValidator = new CopyIntoCastValidator(spark)
  private val dataFrameBuilder = new CopyIntoDataFrameBuilder(spark, fileFormat, columns)

  override def output: Seq[Attribute] = out

  override protected def run(): Seq[InternalRow] = {
    fileFormat.validateForImport()

    val table = catalog.loadTable(ident)
    assert(table.isInstanceOf[SparkTable])
    val paimonTable = table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
    val tableSchema = paimonTable.schema()
    val writableColumns = tableSchema.fieldNames().asScala.toSeq
    val fields = tableSchema.fields().asScala.toSeq
    val targetColumns = CopyIntoHelper.resolveTargetColumns(spark, columns, writableColumns)

    CopyIntoHelper.validateNonNullableDefaults(columns, writableColumns, targetColumns, fields)

    val (filesToLoad, skippedFiles) =
      CopyIntoHelper.listAndFilterFiles(spark, paimonTable, sourcePath, pattern, force)

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

    val selectedDf =
      dataFrameBuilder.buildParquetDataFrame(rawDf, targetColumns, writableColumns, fields)
    castValidator.validateParquetCast(rawDf, targetColumns, writableColumns, fields)

    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    selectedDf.write.format("paimon").mode("append").insertInto(tableName)

    val countDf = spark.read.options(readerOptions).parquet(filePaths: _*)
    recordHistoryAndBuildResults(paimonTable, filesToLoad, skippedFiles, countDf)
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
    val stringSchema = dataFrameBuilder.buildStringSchema(targetColumns)

    val sourceDf = dataFrameBuilder.readSourceData(filePaths, stringSchema, readerOptions)
    val finalDf =
      dataFrameBuilder.buildFinalDataFrame(sourceDf, targetColumns, writableColumns, fields)
    val castedDf = castValidator.castAndValidate(finalDf, writableColumns, fields)

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
}
