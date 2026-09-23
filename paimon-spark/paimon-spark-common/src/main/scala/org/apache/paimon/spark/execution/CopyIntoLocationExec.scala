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
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String

/** The source to export from: either a Paimon table or an inline read-only query. */
sealed trait CopyIntoSource

object CopyIntoSource {

  /** Export an existing Paimon table identified by `catalog`/`ident`. */
  case class TableSource(catalog: TableCatalog, ident: Identifier) extends CopyIntoSource

  /** Export the result of an inline `FROM (<query>)` read-only query. */
  case class QuerySource(query: String) extends CopyIntoSource
}

case class CopyIntoLocationExec(
    spark: SparkSession,
    source: CopyIntoSource,
    targetPath: String,
    fileFormat: CopyFileFormat,
    overwrite: Boolean,
    out: Seq[Attribute])
  extends PaimonLeafV2CommandExec {

  override def output: Seq[Attribute] = out

  override protected def run(): Seq[InternalRow] = {
    fileFormat.validateForExport()

    val df = source match {
      case CopyIntoSource.QuerySource(query) => CopyIntoUtils.queryToDataFrame(spark, query)
      case CopyIntoSource.TableSource(catalog, ident) =>
        spark.table(CopyIntoUtils.quoteIdentifier(catalog.name(), ident))
    }

    val writerOptions = fileFormat.toSparkWriterOptions
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists

    // `rows_written` is counted by a separate `count()` action before the write. The DataFrame is
    // lazy and not cached, so the write re-executes the query a second time; for a non-deterministic
    // query (e.g. `rand()`, `current_timestamp()`, or a volatile source) the two runs can yield
    // different rows, so this count may not match the files (see the docs). We accept this rather
    // than stage the whole result to disk just to make the count exact.
    val rowCount = df.count()
    fileFormat.formatType match {
      case FileFormatType.JSON =>
        df.write.options(writerOptions).mode(saveMode).json(targetPath)
      case FileFormatType.PARQUET =>
        df.write.options(writerOptions).mode(saveMode).parquet(targetPath)
      case _ =>
        df.write.options(writerOptions).mode(saveMode).csv(targetPath)
    }

    val hadoopConf = spark.sessionState.newHadoopConf()
    val fsPath = new Path(targetPath)
    val fs = fsPath.getFileSystem(hadoopConf)
    // Count only data files (part-*); committer side files such as _SUCCESS are not data output.
    val fileCount = if (fs.exists(fsPath)) {
      fs.listStatus(fsPath).count(s => s.isFile && s.getPath.getName.startsWith("part-"))
    } else {
      0
    }

    Seq(InternalRow(UTF8String.fromString(targetPath), fileCount, rowCount))
  }
}
