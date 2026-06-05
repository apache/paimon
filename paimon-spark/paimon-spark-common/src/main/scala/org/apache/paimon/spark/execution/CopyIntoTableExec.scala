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
import org.apache.paimon.spark.catalyst.plans.logical.{CopyFileFormat, FileFormatType, OnErrorMode}
import org.apache.paimon.spark.copyinto.CopyIntoResultBuilder
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataField

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.functions.{col, input_file_name, substring_index}
import org.apache.spark.sql.types.{StringType, StructField}

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
    onError: OnErrorMode,
    out: Seq[Attribute])
  extends PaimonLeafV2CommandExec
  with Logging {

  // Initialize helper classes
  private val castValidator = new CopyIntoCastValidator(spark)
  private val dataFrameBuilder = new CopyIntoDataFrameBuilder(spark, fileFormat, columns)
  private val errorHandler = new CopyIntoErrorHandler(spark, castValidator, dataFrameBuilder)

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
      return CopyIntoResultBuilder.buildSkippedResults(skippedFiles)
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
   * Unified error-tolerant import for both CONTINUE and SKIP_FILE modes. Both start from
   * `goodRowsDf`, the row-clean DataFrame produced by error detection (bad rows already removed
   * and, for text formats, already cast to target types).
   *   - CONTINUE: writes every good row, so a file with errors is partially loaded.
   *   - SKIP_FILE: additionally drops every row belonging to a file that had any error, so such a
   *     file is loaded all-or-nothing.
   *
   * Both modes use a single batch write (one commit) regardless of file count.
   */
  private def runErrorTolerantMode(
      paimonTable: FileStoreTable,
      rawDf: DataFrame,
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      errorGranularity: ErrorGranularity,
      detectErrors: (DataFrame, String) => ErrorDetectionResult): Seq[InternalRow] = {

    val allTargetCols = writableColumns.toSet ++ targetColumns.toSet
    val inputFileCol = CopyIntoHelper.safeTempCol(spark, "__input_file", allTargetCols)
    val rawDfWithFile = rawDf.withColumn(inputFileCol, input_file_name()).cache()

    try {
      val errorResult = detectErrors(rawDfWithFile, inputFileCol)

      // `goodRowsDf` carries `inputFileCol` (full path); error keys are base names, so compare on
      // the base name extracted from the path.
      val filesWithErrors = errorResult.parseErrors.keySet ++ errorResult.castErrors.keySet
      val dfToWrite = errorGranularity match {
        case ErrorGranularity.RowLevel =>
          // CONTINUE: keep all good rows, including good rows from files that also had errors.
          errorResult.goodRowsDf

        case ErrorGranularity.FileLevel =>
          // SKIP_FILE: drop every row whose file had any error.
          if (filesWithErrors.nonEmpty) {
            val baseName = substring_index(col(inputFileCol), "/", -1)
            errorResult.goodRowsDf.filter(!baseName.isin(filesWithErrors.toSeq: _*))
          } else {
            errorResult.goodRowsDf
          }
      }

      val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
      val finalDf = fileFormat.formatType match {
        case FileFormatType.PARQUET =>
          dataFrameBuilder.buildParquetDataFrame(dfToWrite, targetColumns, writableColumns, fields)
        case _ =>
          // For text formats, goodRowsDf is already processed and cast.
          dfToWrite
      }

      finalDf.drop(inputFileCol).write.format("paimon").mode("append").insertInto(tableName)

      errorHandler.buildErrorTolerantResults(
        paimonTable,
        filesToLoad,
        skippedFiles,
        errorResult,
        filesWithErrors,
        errorGranularity)

    } finally {
      rawDfWithFile.unpersist()
    }
  }

  /**
   * Import files in ABORT mode: read, validate, write, and return per-file row counts. Validation
   * aborts the whole statement on the first parse or cast error, so either all files are written or
   * none are.
   */
  private def importAbort(
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      readerOptions: Map[String, String],
      tableName: String): Map[String, Long] = {
    val allTargetCols = writableColumns.toSet ++ targetColumns.toSet
    val fileCol = CopyIntoHelper.safeTempCol(spark, "__file__", allTargetCols)
    fileFormat.formatType match {
      case FileFormatType.PARQUET =>
        val rawDf = spark.read.options(readerOptions).parquet(filePaths: _*)
        castValidator.validateParquetCast(rawDf, targetColumns, writableColumns, fields)
        val selectedDf =
          dataFrameBuilder
            .buildParquetDataFrame(rawDf, targetColumns, writableColumns, fields)
            .withColumn(fileCol, input_file_name())
            .cache()
        try {
          val counts = CopyIntoUtils.countPerFile(selectedDf, fileCol)
          selectedDf.drop(fileCol).write.format("paimon").mode("append").insertInto(tableName)
          counts
        } finally {
          selectedDf.unpersist()
        }
      case _ =>
        val stringSchema = dataFrameBuilder.buildStringSchema(targetColumns)
        val sourceDf = dataFrameBuilder.readSourceData(filePaths, stringSchema, readerOptions)
        val finalDf =
          dataFrameBuilder.buildFinalDataFrame(sourceDf, targetColumns, writableColumns, fields)
        val castedDf = castValidator
          .castAndValidate(finalDf, writableColumns, fields)
          .withColumn(fileCol, input_file_name())
          .cache()
        try {
          val counts = CopyIntoUtils.countPerFile(castedDf, fileCol)
          castedDf.drop(fileCol).write.format("paimon").mode("append").insertInto(tableName)
          counts
        } finally {
          castedDf.unpersist()
        }
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

    onError match {
      case OnErrorMode.Continue | OnErrorMode.SkipFile =>
        val errorGranularity = if (onError == OnErrorMode.Continue) {
          ErrorGranularity.RowLevel
        } else {
          ErrorGranularity.FileLevel
        }

        runErrorTolerantMode(
          paimonTable,
          rawDf,
          targetColumns,
          writableColumns,
          fields,
          filesToLoad,
          skippedFiles,
          errorGranularity,
          errorHandler.detectParquetErrors(targetColumns, writableColumns, fields)
        )

      case _ =>
        val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
        val countsPerFile =
          importAbort(filePaths, targetColumns, writableColumns, fields, readerOptions, tableName)
        CopyIntoResultBuilder.recordHistoryAndBuildResultsDirect(
          paimonTable,
          filesToLoad,
          skippedFiles,
          countsPerFile)
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
    val stringSchema = dataFrameBuilder.buildStringSchema(targetColumns)

    onError match {
      case OnErrorMode.Continue | OnErrorMode.SkipFile =>
        val errorGranularity = if (onError == OnErrorMode.Continue) {
          ErrorGranularity.RowLevel
        } else {
          ErrorGranularity.FileLevel
        }

        // For error-tolerant modes, we need to read with corrupt record tracking
        val allTargetCols = writableColumns.toSet ++ targetColumns.toSet
        val corruptCol = CopyIntoHelper.safeTempCol(spark, "_corrupt_record", allTargetCols)
        val schemaWithCorrupt =
          stringSchema.add(StructField(corruptCol, StringType, nullable = true))
        val corruptRecordOption =
          Map("columnNameOfCorruptRecord" -> corruptCol, "mode" -> "PERMISSIVE")

        val rawDf = fileFormat.formatType match {
          case FileFormatType.JSON =>
            spark.read
              .options(readerOptions ++ corruptRecordOption)
              .schema(schemaWithCorrupt)
              .json(filePaths: _*)
          case _ =>
            spark.read
              .options(readerOptions ++ corruptRecordOption)
              .schema(schemaWithCorrupt)
              .csv(filePaths: _*)
        }

        runErrorTolerantMode(
          paimonTable,
          rawDf,
          targetColumns,
          writableColumns,
          fields,
          filesToLoad,
          skippedFiles,
          errorGranularity,
          errorHandler.detectTextErrors(
            targetColumns,
            writableColumns,
            fields,
            stringSchema,
            corruptCol)
        )

      case _ =>
        runTextImportAbort(
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

  private def runTextImportAbort(
      paimonTable: FileStoreTable,
      filePaths: Array[String],
      targetColumns: Seq[String],
      writableColumns: Seq[String],
      fields: Seq[DataField],
      filesToLoad: Array[FileStatus],
      skippedFiles: Array[FileStatus],
      readerOptions: Map[String, String]): Seq[InternalRow] = {
    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    val countsPerFile =
      importAbort(filePaths, targetColumns, writableColumns, fields, readerOptions, tableName)

    CopyIntoResultBuilder.recordHistoryAndBuildResultsDirect(
      paimonTable,
      filesToLoad,
      skippedFiles,
      countsPerFile)
  }
}
