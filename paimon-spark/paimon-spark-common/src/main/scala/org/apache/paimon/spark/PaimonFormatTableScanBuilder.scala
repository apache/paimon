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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.table.{FormatTable, Table}

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.asScalaBufferConverter

/** ScanBuilder for FormatTable that supports basic scan operations. */
class PaimonFormatTableScanBuilder(table: FormatTable) extends PaimonScanBuilder(table) {
  override def build(): Scan = {
    PaimonFormatTableScan(table, requiredSchema, pushedPaimonPredicates)
  }
}

case class PaimonFormatTableScan(table: Table, requiredSchema: StructType, filters: Seq[Predicate])
  extends ColumnPruningAndPushDown
  with ScanHelper {

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())
  private val formatDataSplits = readBuilder.newScan().plan().splits().asScala.toArray

  override def toBatch: Batch = {
    PaimonBatch(getInputPartitions(formatDataSplits), readBuilder, metadataColumns)
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    Array(
      PaimonNumSplitMetric(),
      PaimonSplitSizeMetric(),
      PaimonAvgSplitSizeMetric(),
      PaimonResultedTableFilesMetric()
    )
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    val filesCount = formatDataSplits.length
    Array(
      PaimonResultedTableFilesTaskMetric(filesCount)
    )
  }

  override def description(): String = {
    val pushedFiltersStr = if (filters.nonEmpty) {
      ", PushedFilters: [" + filters.mkString(",") + "]"
    } else {
      ""
    }
    s"PaimonFormatTableScan: [${table.name}]" + pushedFiltersStr
  }
}
