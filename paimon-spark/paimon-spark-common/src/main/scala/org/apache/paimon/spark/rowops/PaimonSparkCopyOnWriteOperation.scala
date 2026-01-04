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

package org.apache.paimon.spark.rowops

import org.apache.paimon.options.Options
import org.apache.paimon.spark.PaimonBaseScanBuilder
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.spark.write.PaimonV2WriteBuilder
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperation, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PaimonSparkCopyOnWriteOperation(table: FileStoreTable, info: RowLevelOperationInfo)
  extends RowLevelOperation {

  private var copyOnWriteScan: Option[PaimonCopyOnWriteScan] = None

  override def command(): RowLevelOperation.Command = info.command()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new PaimonBaseScanBuilder {
      override lazy val table: FileStoreTable =
        PaimonSparkCopyOnWriteOperation.this.table.copy(options.asCaseSensitiveMap)

      override def build(): Scan = {
        val scan =
          PaimonCopyOnWriteScan(table, requiredSchema, pushedPartitionFilters, pushedDataFilters)
        PaimonSparkCopyOnWriteOperation.this.copyOnWriteScan = Option(scan)
        scan
      }
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val options = Options.fromMap(info.options)
    val builder = new PaimonV2WriteBuilder(table, info.schema(), options)
    assert(copyOnWriteScan.isDefined)
    builder.overwriteFiles(copyOnWriteScan.get)
  }

  override def requiredMetadataAttributes(): Array[NamedReference] = {
    Array(Expressions.column(FILE_PATH_COLUMN))
  }
}
