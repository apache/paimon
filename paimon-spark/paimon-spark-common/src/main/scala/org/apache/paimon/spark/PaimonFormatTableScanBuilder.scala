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
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.source.Split

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * A ScanBuilder implementation for {@link FormatTable} that supports basic scan operations.
 *
 * <p>This class is a key component of the new format table reading functionality in Paimon Spark.
 * It is responsible for constructing a {@link Scan} for a given {@link FormatTable}, applying
 * required schema and predicate pushdown as needed. The builder leverages the capabilities of
 * {@code FormatTable} to efficiently plan and execute scans over tables with various formats.
 *
 * <p>Relationship to {@code FormatTable}: This builder is specifically designed to work with {@code
 * FormatTable} instances, enabling Spark to read data from tables that support different file
 * formats and scan optimizations provided by the Paimon.
 */
case class PaimonFormatTableScanBuilder(table: FormatTable) extends PaimonScanBuilder(table) {
  override def build() = PaimonFormatTableScan(table, requiredSchema, pushedPaimonPredicates)
}

case class PaimonFormatTableScan(
    table: FormatTable,
    requiredSchema: StructType,
    filters: Seq[Predicate])
  extends ColumnPruningAndPushDown
  with ScanHelper {

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())
  protected var inputSplits: Array[Split] = _

  def getOriginSplits: Array[Split] = {
    if (inputSplits == null) {
      inputSplits = readBuilder
        .newScan()
        .plan()
        .splits()
        .asScala
        .toArray
    }
    inputSplits
  }

  override def toBatch: Batch = {
    PaimonBatch(getInputPartitions(getOriginSplits), readBuilder, metadataColumns)
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
    val filesCount = getOriginSplits.length
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
