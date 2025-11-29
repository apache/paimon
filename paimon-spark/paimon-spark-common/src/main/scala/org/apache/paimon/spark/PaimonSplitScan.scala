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
import org.apache.paimon.table.{InnerTable, KnownSplitsTable}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

import java.lang.{Long => JLong}

class PaimonSplitScanBuilder(table: KnownSplitsTable) extends PaimonScanBuilder(table) {
  override def build(): Scan = {
    PaimonSplitScan(table, table.splits(), requiredSchema, pushedPaimonPredicates, pushedRowIds)
  }
}

/** For internal use only. */
case class PaimonSplitScan(
    table: InnerTable,
    dataSplits: Array[DataSplit],
    requiredSchema: StructType,
    filters: Seq[Predicate],
    override val rowIds: Seq[JLong])
  extends ColumnPruningAndPushDown
  with ScanHelper {

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())

  override def toBatch: Batch = {
    PaimonBatch(
      getInputPartitions(dataSplits.asInstanceOf[Array[Split]]),
      readBuilder,
      coreOptions.blobAsDescriptor(),
      metadataColumns)
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
    val filesCount = dataSplits.map(_.dataFiles().size).sum
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
    s"PaimonSplitScan: [${table.name}]" + pushedFiltersStr
  }
}
