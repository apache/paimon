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

case class CopyIntoLocationExec(
    spark: SparkSession,
    catalog: TableCatalog,
    ident: Identifier,
    targetPath: String,
    fileFormat: CopyFileFormat,
    overwrite: Boolean,
    out: Seq[Attribute])
  extends PaimonLeafV2CommandExec {

  override def output: Seq[Attribute] = out

  override protected def run(): Seq[InternalRow] = {
    fileFormat.validateForExport()

    val tableName = CopyIntoUtils.quoteIdentifier(catalog.name(), ident)
    val df = spark.table(tableName)

    val rowCount = df.count()

    val writerOptions = fileFormat.toSparkWriterOptions
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists

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
    val fileCount = if (fs.exists(fsPath)) {
      fs.listStatus(fsPath).count(_.isFile)
    } else {
      0
    }

    Seq(InternalRow(UTF8String.fromString(targetPath), fileCount, rowCount))
  }
}
