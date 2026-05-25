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

package org.apache.paimon.spark.format

import org.apache.paimon.format.csv.CsvOptions
import org.apache.paimon.spark.{BaseTable, FormatTableScanBuilder}
import org.apache.paimon.spark.write.BaseV2WriteBuilder
import org.apache.paimon.table.FormatTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Locale

case class PaimonFormatTable(table: FormatTable)
  extends BaseTable
  with SupportsRead
  with SupportsWrite {

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, OVERWRITE_DYNAMIC, OVERWRITE_BY_FILTER)
  }

  override def properties: util.Map[String, String] = {
    val properties = new util.HashMap[String, String](table.options())
    properties.put(TableCatalog.PROP_PROVIDER, table.format.name().toLowerCase(Locale.ROOT))
    if (table.comment.isPresent) {
      properties.put(TableCatalog.PROP_COMMENT, table.comment.get)
    }
    if (FormatTable.Format.CSV == table.format) {
      properties.put(
        "sep",
        properties.getOrDefault(
          CsvOptions.FIELD_DELIMITER.key(),
          CsvOptions.FIELD_DELIMITER.defaultValue()))
    }
    properties
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    val scanBuilder = FormatTableScanBuilder(table.copy(caseInsensitiveStringMap))
    scanBuilder.pruneColumns(schema)
    scanBuilder
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    PaimonFormatTableWriterBuilder(table, info.schema)
  }
}

case class PaimonFormatTableWriterBuilder(table: FormatTable, writeSchema: StructType)
  extends BaseV2WriteBuilder(table) {

  override def partitionRowType(): RowType = table.partitionType

  override def build: Write = new Write() {
    override def toBatch: BatchWrite = {
      SparkShimLoader.shim
        .createFormatTableBatchWrite(table, overwriteDynamic, overwritePartitions, writeSchema)
    }

    override def toStreaming: StreamingWrite = {
      throw new UnsupportedOperationException("FormatTable doesn't support streaming write")
    }
  }
}
